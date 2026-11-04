import { Injectable, Logger, OnApplicationBootstrap } from '@nestjs/common';
import type { ConfirmChannel, ConsumeMessage } from 'amqplib';
import { DatabaseService } from '../database/database.service.js';
import { RabbitmqService } from '../mensajeria/rabbitmq.service.js';
import { QUEUES } from '../mensajeria/topologia.js';

@Injectable()
export class CartasMuertasConsumidor implements OnApplicationBootstrap {
  private readonly logger = new Logger(CartasMuertasConsumidor.name);

  constructor(private readonly database: DatabaseService, private readonly rabbit: RabbitmqService) {}

  onApplicationBootstrap(): void { this.rabbit.registrarConsumidor((channel) => this.iniciar(channel)); }

  private iniciar(channel: ConfirmChannel): void {
    for (const cola of [QUEUES.AVISOS_DLQ, QUEUES.AUDITORIA_DLQ, QUEUES.CORREOS_DLQ]) {
      channel.consume(cola, (msg) => void this.procesar(channel, cola, msg), { noAck: false });
    }
  }

  private async procesar(channel: ConfirmChannel, colaDlq: string, msg: ConsumeMessage | null): Promise<void> {
    if (!msg) return;
    try {
      const xDeath = msg.properties.headers?.['x-death'] as Array<Record<string, unknown>> | undefined;
      const deadLetter = xDeath?.[0];
      const routingKeys = deadLetter?.['routing-keys'] as string[] | undefined;
      const colaOrigen = typeof deadLetter?.queue === 'string' ? deadLetter.queue : colaDlq;
      const intentos = Number(deadLetter?.count ?? 1);
      const motivo = String(deadLetter?.reason ?? 'nack');
      const routingKey = routingKeys?.[0] ?? msg.fields.routingKey;
      await this.database.query(
        `INSERT INTO mensajes_muertos (cola_origen, routing_key, motivo, intentos, payload)
         VALUES ($1, $2, $3, $4, $5)`,
        [colaOrigen, routingKey, motivo, intentos, msg.content.toString()],
      );
      this.logger.log(`[DLQ] registrada origen=${colaOrigen} routingKey=${routingKey} motivo=${motivo} intentos=${intentos}`);
      channel.ack(msg);
    } catch (error) {
      this.logger.error(`[DLQ ${colaDlq}] falla:`, error);
      setTimeout(() => channel.nack(msg, false, true), 5000);
    }
  }
}
