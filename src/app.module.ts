import { Module, Controller, Get, OnModuleInit, Logger } from '@nestjs/common';
import { randomUUID } from 'crypto';
import amqp, { Channel, ChannelModel } from 'amqplib';
import pg from 'pg';
import {
  EXCHANGES,
  QUEUES,
  BINDINGS,
  DLX_ROUTING_KEYS,
} from './mensajeria/topologia.js';

const { Pool } = pg;
const MAX_INTENTOS = 3;

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
  private healthCtrl?: HealthController;

  async onModuleInit() {
    await this.initTablas();
    this.iniciarConsumidor();
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

  private async iniciarConsumidor() {
    const url = process.env.RABBITMQ_URL || 'amqp://vidalstore:vidalstore@rabbitmq:5672';
    try {
      const conn: ChannelModel = await amqp.connect(url);
      const ch: Channel = await conn.createChannel();
      ch.prefetch(1);
      if (this.healthCtrl) this.healthCtrl.setRabbitOk(true);

      await ch.assertExchange(EXCHANGES.EVENTOS, 'topic',  { durable: true });
      await ch.assertExchange(EXCHANGES.DLX,     'direct', { durable: true });

      for (const [q, rk] of [
        [QUEUES.AVISOS_DLQ,    DLX_ROUTING_KEYS.AVISOS],
        [QUEUES.AUDITORIA_DLQ, DLX_ROUTING_KEYS.AUDITORIA],
        [QUEUES.CORREOS_DLQ,   DLX_ROUTING_KEYS.CORREOS],
      ] as [string, string][]) {
        await ch.assertQueue(q, { durable: true });
        ch.bindQueue(q, EXCHANGES.DLX, rk);
      }

      const dlxArgs = (rk: string) => ({
        'x-dead-letter-exchange':    EXCHANGES.DLX,
        'x-dead-letter-routing-key': rk,
      });
      await ch.assertQueue(QUEUES.AVISOS,    { durable: true, arguments: dlxArgs(DLX_ROUTING_KEYS.AVISOS) });
      await ch.assertQueue(QUEUES.AUDITORIA, { durable: true, arguments: dlxArgs(DLX_ROUTING_KEYS.AUDITORIA) });
      await ch.assertQueue(QUEUES.CORREOS,   { durable: true, arguments: dlxArgs(DLX_ROUTING_KEYS.CORREOS) });

      ch.bindQueue(QUEUES.AVISOS,    EXCHANGES.EVENTOS, BINDINGS.AVISOS_PATTERN);
      ch.bindQueue(QUEUES.AUDITORIA, EXCHANGES.EVENTOS, BINDINGS.AUDITORIA_PATTERN);
      ch.bindQueue(QUEUES.CORREOS,   EXCHANGES.EVENTOS, BINDINGS.CORREOS_PATTERN);

      this.logger.log('Topología RabbitMQ configurada');

      ch.consume(QUEUES.AUDITORIA, async (msg) => {
        if (!msg) return;
        const routingKey = msg.fields.routingKey;
        try {
          const raw = msg.content.toString();
          let payload: unknown;
          try { payload = JSON.parse(raw); }
          catch { ch.nack(msg, false, false); return; } // malformado → DLX sin reintento

          const eventoId  = (msg.properties.headers?.['x-evento-id']  as string) ?? randomUUID();
          const emitidoEn = (msg.properties.headers?.['x-emitido-en'] as string) ?? null;
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
          const intentos = ((msg.properties.headers?.['x-death'] as any[])?.[0]?.count ?? 0) + 1;
          ch.nack(msg, false, intentos < MAX_INTENTOS); // reintento acotado
        }
      });

      ch.consume(QUEUES.AVISOS, async (msg) => {
        if (!msg) return;
        try {
          const raw = msg.content.toString();
          let payload: any;
          try { payload = JSON.parse(raw); }
          catch { ch.nack(msg, false, false); return; }

          const eventoId  = (msg.properties.headers?.['x-evento-id'] as string) ?? randomUUID();
          const para = payload.usuario_sub ?? payload.usuarioSub ?? 'desconocido';

          await this.pool.query(
            `INSERT INTO notificaciones (para, asunto, estado, evento_id)
             VALUES ($1, $2, 'enviada', $3)`,
            [para, `Aviso: ${msg.fields.routingKey}`, eventoId],
          );
          ch.ack(msg);
        } catch (err) {
          this.logger.error('[AVISOS] falla:', err);
          const intentos = ((msg.properties.headers?.['x-death'] as any[])?.[0]?.count ?? 0) + 1;
          ch.nack(msg, false, intentos < MAX_INTENTOS);
        }
      });

      ch.consume(QUEUES.CORREOS, async (msg) => {
        if (!msg) return;
        try {
          const raw = msg.content.toString();
          let payload: any;
          try { payload = JSON.parse(raw); }
          catch { ch.nack(msg, false, false); return; }

          const eventoId = (msg.properties.headers?.['x-evento-id'] as string) ?? randomUUID();
          const para = payload.usuario_sub ?? payload.usuarioSub ?? 'desconocido';

          await this.pool.query(
            `INSERT INTO notificaciones (para, asunto, estado, evento_id)
             VALUES ($1, 'Confirmación de compra', 'enviada', $2)`,
            [para, eventoId],
          );
          ch.ack(msg);
        } catch (err) {
          this.logger.error('[CORREOS] falla:', err);
          const intentos = ((msg.properties.headers?.['x-death'] as any[])?.[0]?.count ?? 0) + 1;
          ch.nack(msg, false, intentos < MAX_INTENTOS);
        }
      });

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
            ch.nack(msg, false, false);
          }
        });
      }

      conn.on('error', () => { if (this.healthCtrl) this.healthCtrl.setRabbitOk(false); });
    } catch (err) {
      this.logger.error('Error conectando a RabbitMQ, reintentando en 5s...', err);
      if (this.healthCtrl) this.healthCtrl.setRabbitOk(false);
      setTimeout(() => this.iniciarConsumidor(), 5000);
    }
  }
}
