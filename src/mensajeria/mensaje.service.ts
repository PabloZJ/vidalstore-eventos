import { Injectable } from '@nestjs/common';
import type { ConfirmChannel, ConsumeMessage } from 'amqplib';
import { EXCHANGES } from './topologia.js';

export class MensajeInvalido extends Error {}

@Injectable()
export class MensajeService {
  private static readonly maxIntentos = 3;

  eventoId(msg: ConsumeMessage): string {
    const value = msg.properties.headers?.['x-evento-id'];
    if (typeof value !== 'string' || !/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value)) {
      throw new MensajeInvalido('El mensaje no trae un x-evento-id UUID valido');
    }
    return value;
  }

  emitidoEn(msg: ConsumeMessage): string {
    const value = msg.properties.headers?.['x-emitido-en'];
    if (typeof value !== 'string' || Number.isNaN(Date.parse(value))) {
      throw new MensajeInvalido('El mensaje no trae un x-emitido-en ISO-8601 valido');
    }
    return value;
  }

  destinatario(payload: unknown): string {
    if (!payload || typeof payload !== 'object') throw new MensajeInvalido('El payload no es un objeto');
    const values = payload as Record<string, unknown>;
    const value = values.usuario_sub ?? values.usuarioSub ?? values.para;
    if (typeof value !== 'string' || value.length === 0) throw new MensajeInvalido('El mensaje no trae destinatario');
    return value;
  }

  async reintentar(channel: ConfirmChannel, msg: ConsumeMessage): Promise<void> {
    const headers = { ...(msg.properties.headers ?? {}) } as Record<string, unknown>;
    const intentos = Number(headers['x-reintentos'] ?? 0) + 1;
    if (intentos >= MensajeService.maxIntentos) {
      channel.nack(msg, false, false);
      return;
    }

    headers['x-reintentos'] = intentos;
    channel.publish(msg.fields.exchange || EXCHANGES.EVENTOS.nombre, msg.fields.routingKey, msg.content, {
      persistent: true, contentType: msg.properties.contentType ?? 'application/json',
      messageId: msg.properties.messageId, headers,
    });
    await channel.waitForConfirms();
    channel.ack(msg);
  }
}
