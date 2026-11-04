import { Injectable, Logger, OnApplicationBootstrap } from '@nestjs/common';
import type { ConfirmChannel, ConsumeMessage } from 'amqplib';
import { DatabaseService } from '../database/database.service.js';
import { MensajeInvalido, MensajeService } from '../mensajeria/mensaje.service.js';
import { RabbitmqService } from '../mensajeria/rabbitmq.service.js';
import { EXCHANGES, QUEUES, ROUTING_KEYS } from '../mensajeria/topologia.js';

@Injectable()
export class AvisosConsumidor implements OnApplicationBootstrap {
  private readonly logger = new Logger(AvisosConsumidor.name);

  constructor(private readonly database: DatabaseService, private readonly mensajes: MensajeService, private readonly rabbit: RabbitmqService) {}

  onApplicationBootstrap(): void { this.rabbit.registrarConsumidor((channel) => this.iniciar(channel)); }

  private iniciar(channel: ConfirmChannel): void {
    channel.consume(QUEUES.AVISOS, (msg) => void this.procesar(channel, msg), { noAck: false });
  }

  private async procesar(channel: ConfirmChannel, msg: ConsumeMessage | null): Promise<void> {
    if (!msg) return;
    try {
      const payload = JSON.parse(msg.content.toString());
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
      if (error instanceof SyntaxError || error instanceof MensajeInvalido) channel.nack(msg, false, false);
      else await this.mensajes.reintentar(channel, msg);
    }
  }
}
