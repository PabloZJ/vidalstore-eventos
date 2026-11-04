import { Injectable, Logger, OnApplicationBootstrap } from '@nestjs/common';
import type { ConfirmChannel, ConsumeMessage } from 'amqplib';
import { DatabaseService } from '../database/database.service.js';
import { MensajeInvalido, MensajeService } from '../mensajeria/mensaje.service.js';
import { RabbitmqService } from '../mensajeria/rabbitmq.service.js';
import { QUEUES } from '../mensajeria/topologia.js';

@Injectable()
export class AuditoriaConsumidor implements OnApplicationBootstrap {
  private readonly logger = new Logger(AuditoriaConsumidor.name);

  constructor(private readonly database: DatabaseService, private readonly mensajes: MensajeService, private readonly rabbit: RabbitmqService) {}

  onApplicationBootstrap(): void { this.rabbit.registrarConsumidor((channel) => this.iniciar(channel)); }

  private iniciar(channel: ConfirmChannel): void {
    channel.consume(QUEUES.AUDITORIA, (msg) => void this.procesar(channel, msg), { noAck: false });
  }

  private async procesar(channel: ConfirmChannel, msg: ConsumeMessage | null): Promise<void> {
    if (!msg) return;
    const routingKey = msg.fields.routingKey;
    try {
      const payload = JSON.parse(msg.content.toString());
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
      if (error instanceof SyntaxError || error instanceof MensajeInvalido) channel.nack(msg, false, false);
      else await this.mensajes.reintentar(channel, msg);
    }
  }
}
