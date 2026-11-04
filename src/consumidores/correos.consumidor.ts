import { Injectable, Logger, OnApplicationBootstrap } from '@nestjs/common';
import type { ConfirmChannel, ConsumeMessage } from 'amqplib';
import { DatabaseService } from '../database/database.service.js';
import { MensajeInvalido, MensajeService } from '../mensajeria/mensaje.service.js';
import { RabbitmqService } from '../mensajeria/rabbitmq.service.js';
import { QUEUES } from '../mensajeria/topologia.js';

@Injectable()
export class CorreosConsumidor implements OnApplicationBootstrap {
  private readonly logger = new Logger(CorreosConsumidor.name);

  constructor(private readonly database: DatabaseService, private readonly mensajes: MensajeService, private readonly rabbit: RabbitmqService) {}

  onApplicationBootstrap(): void { this.rabbit.registrarConsumidor((channel) => this.iniciar(channel)); }

  private iniciar(channel: ConfirmChannel): void {
    channel.consume(QUEUES.CORREOS, (msg) => void this.procesar(channel, msg), { noAck: false });
  }

  private async procesar(channel: ConfirmChannel, msg: ConsumeMessage | null): Promise<void> {
    if (!msg) return;
    try {
      const payload = JSON.parse(msg.content.toString()) as Record<string, unknown>;
      const eventoId = this.mensajes.eventoId(msg);
      this.mensajes.emitidoEn(msg);
      const para = this.mensajes.destinatario(payload);
      if (typeof payload.asunto !== 'string' || payload.asunto.length === 0) throw new MensajeInvalido('El comando no trae asunto');
      await this.database.query(
        `INSERT INTO notificaciones (para, asunto, estado, evento_id)
         VALUES ($1, $2, 'enviada', $3) ON CONFLICT (evento_id, asunto) DO NOTHING`,
        [para, payload.asunto, eventoId],
      );
      channel.ack(msg);
    } catch (error) {
      this.logger.error('[CORREOS] falla:', error);
      if (error instanceof SyntaxError || error instanceof MensajeInvalido) channel.nack(msg, false, false);
      else await this.mensajes.reintentar(channel, msg);
    }
  }
}
