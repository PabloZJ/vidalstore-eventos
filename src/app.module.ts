import { Module, Controller, Get, OnModuleInit, Logger, Inject } from '@nestjs/common';
import amqp, { Channel, ChannelModel } from 'amqplib';
import pg from 'pg';
import {
  EXCHANGES,
  QUEUES,
  ROUTING_KEYS,
  declararTopologia,
} from './mensajeria/topologia.js';

const { Pool } = pg;
const MAX_INTENTOS = 3;

class MensajeInvalido extends Error {}

function crearPool() {
  return new Pool({
    host:     process.env.DB_HOST     || 'postgres',
    port:     Number(process.env.DB_PORT) || 5432,
    user:     process.env.DB_USER     || 'vidalstore',
    password: process.env.DB_PASSWORD || 'vidalstore',
    database: process.env.DB_NAME     || 'vidalstore',
  });
}

@Controller()
export class HealthController {
  private pool = crearPool();
  private rabbitOk = false;

  setRabbitOk(v: boolean) { this.rabbitOk = v; }

  @Get()
  async check() {
    let dbOk = false;
    try { await this.pool.query('SELECT 1'); dbOk = true; } catch {}
    const status = dbOk && this.rabbitOk ? 'ok' : 'degradado';
    return { status, service: 'vidalstore-eventos', db: dbOk, rabbit: this.rabbitOk, timestamp: new Date().toISOString() };
  }

  @Get('salud')
  async salud() { return this.check(); }
}

@Module({ controllers: [HealthController] })
export class AppModule implements OnModuleInit {
  private readonly logger = new Logger(AppModule.name);
  private pool = crearPool();

  constructor(@Inject(HealthController) private readonly healthCtrl: HealthController) {}

  async onModuleInit() {
    await this.initTablas();
    this.iniciarConsumidores();
  }

  private async initTablas() {
    await this.pool.query(`
      CREATE TABLE IF NOT EXISTS eventos_auditoria (
        id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        evento_id   UUID UNIQUE NOT NULL,
        routing_key VARCHAR(100) NOT NULL,
        usuario_sub VARCHAR(200),
        payload     JSONB NOT NULL,
        emitido_en  TIMESTAMPTZ,
        recibido_en TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );

       CREATE TABLE IF NOT EXISTS notificaciones (
        id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        para       VARCHAR(200) NOT NULL,
        asunto     TEXT NOT NULL,
        estado     VARCHAR(20) NOT NULL DEFAULT 'enviada' CHECK (estado IN ('enviada','fallida')),
        evento_id  UUID NOT NULL,
         creada_en  TIMESTAMPTZ NOT NULL DEFAULT NOW()
       );

       CREATE UNIQUE INDEX IF NOT EXISTS notificaciones_evento_tipo_unico
         ON notificaciones (evento_id, asunto);

      CREATE TABLE IF NOT EXISTS mensajes_muertos (
        id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        cola_origen VARCHAR(100) NOT NULL,
        routing_key VARCHAR(100) NOT NULL,
        motivo      TEXT,
        intentos    INTEGER NOT NULL DEFAULT 1,
        payload     TEXT NOT NULL,
        recibido_en TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
    `);
    this.logger.log('Tablas de vidalstore-eventos verificadas/creadas');
  }

  private async iniciarConsumidores() {
    const url = process.env.RABBITMQ_URL || 'amqp://vidalstore:vidalstore@rabbitmq:5672';
    try {
      const conn: ChannelModel = await amqp.connect(url);
      const ch: Channel = await conn.createChannel();
      ch.prefetch(1);
      this.healthCtrl.setRabbitOk(true);

       await declararTopologia(ch);

      this.logger.log('Topología RabbitMQ configurada');

      this.consumirAuditoria(ch);
      this.consumirAvisos(ch);
      this.consumirCorreos(ch);
      this.consumirDlq(ch);

      conn.on('error', () => this.healthCtrl.setRabbitOk(false));
    } catch (err) {
      this.logger.error('Error conectando a RabbitMQ, reintentando en 5s...', err);
      this.healthCtrl.setRabbitOk(false);
      setTimeout(() => this.iniciarConsumidores(), 5000);
    }
  }

  private consumirAuditoria(ch: Channel) {
    ch.consume(QUEUES.AUDITORIA, async (msg) => {
        if (!msg) return;
        const routingKey = msg.fields.routingKey;
        try {
          const raw = msg.content.toString();
          let payload: unknown;
          try { payload = JSON.parse(raw); }
          catch { ch.nack(msg, false, false); return; } // malformado → DLX sin reintento

           const eventoId = this.eventoId(msg);
           const emitidoEn = this.emitidoEn(msg);
          const usuarioSub = (msg.properties.headers?.['x-usuario-sub'] as string) ?? null;

          await this.pool.query(
            `INSERT INTO eventos_auditoria (evento_id, routing_key, usuario_sub, payload, emitido_en)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (evento_id) DO NOTHING`,
            [eventoId, routingKey, usuarioSub, JSON.stringify(payload), emitidoEn],
          );
          ch.ack(msg);
        } catch (err) {
          this.logger.error(`[AUDITORIA] falla procesando ${routingKey}:`, err);
          if (err instanceof MensajeInvalido) { ch.nack(msg, false, false); return; }
          this.reintentar(ch, msg);
        }
    });
  }

  private consumirAvisos(ch: Channel) {
    ch.consume(QUEUES.AVISOS, async (msg) => {
        if (!msg) return;
        try {
          const raw = msg.content.toString();
          let payload: any;
          try { payload = JSON.parse(raw); }
          catch { ch.nack(msg, false, false); return; }

           const eventoId = this.eventoId(msg);
           this.emitidoEn(msg);
           const para = this.destinatario(payload);
           const asunto = `Aviso: ${msg.fields.routingKey}`;

          await this.pool.query(
             `INSERT INTO notificaciones (para, asunto, estado, evento_id)
              VALUES ($1, $2, 'enviada', $3)
              ON CONFLICT (evento_id, asunto) DO NOTHING`,
             [para, asunto, eventoId],
           );
           ch.publish(EXCHANGES.COMANDOS.nombre, ROUTING_KEYS.CORREO_ENVIAR, Buffer.from(JSON.stringify({
             para,
             asunto,
             eventoIdOrigen: eventoId,
           })), {
             persistent: true,
             contentType: 'application/json',
             headers: { 'x-evento-id': eventoId, 'x-emitido-en': new Date().toISOString() },
           });
          ch.ack(msg);
        } catch (err) {
          this.logger.error('[AVISOS] falla:', err);
          if (err instanceof MensajeInvalido) { ch.nack(msg, false, false); return; }
          this.reintentar(ch, msg);
        }
    });
  }

  private consumirCorreos(ch: Channel) {
    ch.consume(QUEUES.CORREOS, async (msg) => {
        if (!msg) return;
        try {
          const raw = msg.content.toString();
          let payload: any;
          try { payload = JSON.parse(raw); }
          catch { ch.nack(msg, false, false); return; }

           const eventoId = this.eventoId(msg);
           this.emitidoEn(msg);
           const para = this.destinatario(payload);
            if (typeof payload.asunto !== 'string' || payload.asunto.length === 0) throw new MensajeInvalido('El comando no trae asunto');

          await this.pool.query(
             `INSERT INTO notificaciones (para, asunto, estado, evento_id)
              VALUES ($1, $2, 'enviada', $3)
              ON CONFLICT (evento_id, asunto) DO NOTHING`,
             [para, payload.asunto, eventoId],
          );
          ch.ack(msg);
        } catch (err) {
          this.logger.error('[CORREOS] falla:', err);
          if (err instanceof MensajeInvalido) { ch.nack(msg, false, false); return; }
          this.reintentar(ch, msg);
        }
    });
  }

  private consumirDlq(ch: Channel) {
    for (const cola of [QUEUES.AVISOS_DLQ, QUEUES.AUDITORIA_DLQ, QUEUES.CORREOS_DLQ]) {
      ch.consume(cola, async (msg) => {
          if (!msg) return;
          try {
            const xDeath = msg.properties.headers?.['x-death'] as any[];
            const intentos = xDeath?.[0]?.count ?? 1;
            const motivo   = xDeath?.[0]?.reason ?? 'nack';
            await this.pool.query(
              `INSERT INTO mensajes_muertos (cola_origen, routing_key, motivo, intentos, payload)
               VALUES ($1, $2, $3, $4, $5)`,
              [cola, msg.fields.routingKey, motivo, intentos, msg.content.toString()],
            );
            ch.ack(msg);
          } catch (err) {
            this.logger.error(`[DLQ ${cola}] falla:`, err);
            // No perder la evidencia de un fallo si PostgreSQL no esta disponible.
            ch.nack(msg, false, true);
          }
      });
    }
  }

  /**
   * Re-publica el mensaje con un contador persistente en sus headers.
   * x-death no aumenta con nack(..., requeue=true), por lo que no sirve para
   * acotar reintentos. Al agotarse el límite se rechaza hacia su DLQ.
   */
  private reintentar(ch: Channel, msg: import('amqplib').ConsumeMessage) {
    const headers = { ...(msg.properties.headers ?? {}) } as Record<string, unknown>;
    const intentos = Number(headers['x-reintentos'] ?? 0) + 1;
    if (intentos >= MAX_INTENTOS) {
      ch.nack(msg, false, false);
      return;
    }

    headers['x-reintentos'] = intentos;
     const exchange = msg.fields.exchange || EXCHANGES.EVENTOS.nombre;
     ch.publish(exchange, msg.fields.routingKey, msg.content, {
      persistent: true,
      contentType: msg.properties.contentType ?? 'application/json',
      messageId: msg.properties.messageId,
      headers,
    });
     ch.ack(msg);
   }

  private eventoId(msg: import('amqplib').ConsumeMessage): string {
    const value = msg.properties.headers?.['x-evento-id'];
    if (typeof value !== 'string' || !/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value)) {
      throw new MensajeInvalido('El mensaje no trae un x-evento-id UUID valido');
    }
    return value;
  }

  private emitidoEn(msg: import('amqplib').ConsumeMessage): string {
    const value = msg.properties.headers?.['x-emitido-en'];
    if (typeof value !== 'string' || Number.isNaN(Date.parse(value))) {
      throw new MensajeInvalido('El mensaje no trae un x-emitido-en ISO-8601 valido');
    }
    return value;
  }

  private destinatario(payload: unknown): string {
    if (!payload || typeof payload !== 'object') throw new MensajeInvalido('El payload no es un objeto');
    const value = (payload as Record<string, unknown>).usuario_sub ?? (payload as Record<string, unknown>).usuarioSub ?? (payload as Record<string, unknown>).para;
    if (typeof value !== 'string' || value.length === 0) throw new MensajeInvalido('El mensaje no trae destinatario');
    return value;
  }
}
