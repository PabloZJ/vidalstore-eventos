import type { Channel, Options } from 'amqplib';

export const EXCHANGES = {
  EVENTOS: { nombre: 'vidalstore.eventos', tipo: 'topic' },
  COMANDOS: { nombre: 'vidalstore.comandos', tipo: 'direct' },
  DLX: { nombre: 'vidalstore.dlx', tipo: 'direct' },
} as const;

export const ROUTING_KEYS = {
  JUEGO_PUBLICADO:   'juego.publicado',
  COMPRA_REALIZADA:  'compra.realizada',
  LICENCIA_REVOCADA: 'licencia.revocada',
  CORREO_ENVIAR:     'correo.enviar',
} as const;

export const QUEUES = {
  AVISOS:    'vidalstore.avisos',
  AUDITORIA: 'vidalstore.auditoria',
  CORREOS:   'vidalstore.correos',

  AVISOS_DLQ:    'vidalstore.avisos.dlq',
  AUDITORIA_DLQ: 'vidalstore.auditoria.dlq',
  CORREOS_DLQ:   'vidalstore.correos.dlq',
} as const;

export const BINDINGS = {
  AVISOS_PATTERN:    'licencia.*',
  AUDITORIA_PATTERN: '#',
  CORREOS_PATTERN:   ROUTING_KEYS.CORREO_ENVIAR,
} as const;

export function argumentosDeCola(routingKeyDlq: string): Options.AssertQueue {
  return {
    durable: true,
    arguments: {
      'x-queue-type': 'quorum',
      'x-dead-letter-exchange': EXCHANGES.DLX.nombre,
      'x-dead-letter-routing-key': routingKeyDlq,
    },
  };
}

export async function declararTopologia(canal: Channel): Promise<void> {
  for (const exchange of Object.values(EXCHANGES)) {
    await canal.assertExchange(exchange.nombre, exchange.tipo, { durable: true });
  }

  const colas = [
    [QUEUES.AVISOS, QUEUES.AVISOS_DLQ, DLX_ROUTING_KEYS.AVISOS],
    [QUEUES.AUDITORIA, QUEUES.AUDITORIA_DLQ, DLX_ROUTING_KEYS.AUDITORIA],
    [QUEUES.CORREOS, QUEUES.CORREOS_DLQ, DLX_ROUTING_KEYS.CORREOS],
  ] as const;

  // Las DLQ se declaran antes de las colas de trabajo para no perder rechazos.
  for (const [, dlq, routingKeyDlq] of colas) {
    await canal.assertQueue(dlq, { durable: true, arguments: { 'x-queue-type': 'quorum' } });
    await canal.bindQueue(dlq, EXCHANGES.DLX.nombre, routingKeyDlq);
  }

  for (const [cola, , routingKeyDlq] of colas) {
    await canal.assertQueue(cola, argumentosDeCola(routingKeyDlq));
  }

  await canal.bindQueue(QUEUES.AVISOS, EXCHANGES.EVENTOS.nombre, BINDINGS.AVISOS_PATTERN);
  await canal.bindQueue(QUEUES.AUDITORIA, EXCHANGES.EVENTOS.nombre, BINDINGS.AUDITORIA_PATTERN);
  await canal.bindQueue(QUEUES.CORREOS, EXCHANGES.COMANDOS.nombre, BINDINGS.CORREOS_PATTERN);
}

export const DLX_ROUTING_KEYS = {
  AVISOS:    'vidalstore.avisos.dead',
  AUDITORIA: 'vidalstore.auditoria.dead',
  CORREOS:   'vidalstore.correos.dead',
} as const;
