import { Injectable, Logger } from '@nestjs/common';
import type { ConfirmChannel, ConsumeMessage } from 'amqplib';
import { DatabaseService } from '../database/database.service.js';
import { MensajeInvalido, MensajeService } from '../mensajeria/mensaje.service.js';
import { EXCHANGES, QUEUES, ROUTING_KEYS } from '../mensajeria/topologia.js';

@Injectable()
export class ConsumidoresService {
  private readonly logger = new Logger(ConsumidoresService.name);

  constructor(
    private readonly database: DatabaseService,
    private readonly mensajes: MensajeService,
  ) {}

  iniciar(channel: ConfirmChannel): void {
    this.consumirAuditoria(channel);
    this.consumirAvisos(channel);
    this.consumirCorreos(channel);
    this.consumirDlq(channel);
  }

  private consumirAuditoria(channel: ConfirmChannel): void {
    channel.consume(QUEUES.AUDITORIA, async (msg) => {
      if (!msg) return;
      const routingKey = msg.fields.routingKey;
      try {
        const payload = this.parsearJson(msg, channel);
        if (!payload) return;
        const eventoId = this.mensajes.eventoId(msg);
        const emitidoEn = this.mensajes.emitidoEn(msg);
        const usuarioSub = (msg.properties.headers?.['x-usuario-sub'] as string) ?? null;
        await this.database.query(
          `INSERT INTO eventos_auditoria (evento_id, routing_key, usuario_sub, payload, emitido_en)
           VALUES ($1, $2, $3, $4, $5) ON CONFLICT (evento_id) DO NOTHING`,
          [eventoId, routingKey, usuarioSub, JSON.stringify(payload), emitidoEn],
        );
        channel.ack(msg);
      } catch (error) {
        this.logger.error(`[AUDITORIA] falla procesando ${routingKey}:`, error);
        if (error instanceof MensajeInvalido) channel.nack(msg, false, false);
        else await this.mensajes.reintentar(channel, msg);
      }
    }, { noAck: false });
  }

  private consumirAvisos(channel: ConfirmChannel): void {
    channel.consume(QUEUES.AVISOS, async (msg) => {
      if (!msg) return;
      try {
        const payload = this.parsearJson(msg, channel);
        if (!payload) return;
        const eventoId = this.mensajes.eventoId(msg);
        this.mensajes.emitidoEn(msg);
        const para = this.mensajes.destinatario(payload);
        const asunto = `Aviso: ${msg.fields.routingKey}`;
        await this.database.query(
          `INSERT INTO notificaciones (para, asunto, estado, evento_id)
           VALUES ($1, $2, 'enviada', $3) ON CONFLICT (evento_id, asunto) DO NOTHING`,
          [para, asunto, eventoId],
        );
        channel.publish(EXCHANGES.COMANDOS.nombre, ROUTING_KEYS.CORREO_ENVIAR, Buffer.from(JSON.stringify({ para, asunto, eventoIdOrigen: eventoId })), {
          persistent: true, contentType: 'application/json',
          headers: { 'x-evento-id': eventoId, 'x-emitido-en': new Date().toISOString() },
        });
        await channel.waitForConfirms();
        channel.ack(msg);
      } catch (error) {
        this.logger.error('[AVISOS] falla:', error);
        if (error instanceof MensajeInvalido) channel.nack(msg, false, false);
        else await this.mensajes.reintentar(channel, msg);
      }
    }, { noAck: false });
  }

  private consumirCorreos(channel: ConfirmChannel): void {
    channel.consume(QUEUES.CORREOS, async (msg) => {
      if (!msg) return;
      try {
        const payload = this.parsearJson(msg, channel);
        if (!payload) return;
        const eventoId = this.mensajes.eventoId(msg);
        this.mensajes.emitidoEn(msg);
        const para = this.mensajes.destinatario(payload);
        const asunto = (payload as Record<string, unknown>).asunto;
        if (typeof asunto !== 'string' || asunto.length === 0) throw new MensajeInvalido('El comando no trae asunto');
        await this.database.query(
          `INSERT INTO notificaciones (para, asunto, estado, evento_id)
           VALUES ($1, $2, 'enviada', $3) ON CONFLICT (evento_id, asunto) DO NOTHING`,
          [para, asunto, eventoId],
        );
        channel.ack(msg);
      } catch (error) {
        this.logger.error('[CORREOS] falla:', error);
        if (error instanceof MensajeInvalido) channel.nack(msg, false, false);
        else await this.mensajes.reintentar(channel, msg);
      }
    }, { noAck: false });
  }

  private consumirDlq(channel: ConfirmChannel): void {
    for (const cola of [QUEUES.AVISOS_DLQ, QUEUES.AUDITORIA_DLQ, QUEUES.CORREOS_DLQ]) {
      channel.consume(cola, async (msg) => {
        if (!msg) return;
        try {
          const xDeath = msg.properties.headers?.['x-death'] as Array<Record<string, unknown>> | undefined;
          const deadLetter = xDeath?.[0];
          const routingKeys = deadLetter?.['routing-keys'] as string[] | undefined;
          const intentos = Number(deadLetter?.count ?? 1);
          const motivo = String(deadLetter?.reason ?? 'nack');
          const routingKey = routingKeys?.[0] ?? msg.fields.routingKey;
          await this.database.query(
            `INSERT INTO mensajes_muertos (cola_origen, routing_key, motivo, intentos, payload)
             VALUES ($1, $2, $3, $4, $5)`,
            [cola, routingKey, motivo, intentos, msg.content.toString()],
          );
          this.logger.log(`[DLQ] registrada cola=${cola} routingKey=${routingKey} motivo=${motivo} intentos=${intentos}`);
          channel.ack(msg);
        } catch (error) {
          this.logger.error(`[DLQ ${cola}] falla:`, error);
          channel.nack(msg, false, true);
        }
      }, { noAck: false });
    }
  }

  private parsearJson(msg: ConsumeMessage, channel: ConfirmChannel): unknown | null {
    try {
      return JSON.parse(msg.content.toString());
    } catch {
      channel.nack(msg, false, false);
      return null;
    }
  }
}
