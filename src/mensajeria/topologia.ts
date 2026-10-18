export const EXCHANGES = {
  EVENTOS: 'vidalstore.eventos',
  COMANDOS: 'vidalstore.comandos',
  DLX: 'vidalstore.dlx',
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
  CORREOS_PATTERN:   'compra.realizada',
} as const;

export const DLX_ROUTING_KEYS = {
  AVISOS:    'vidalstore.avisos.dead',
  AUDITORIA: 'vidalstore.auditoria.dead',
  CORREOS:   'vidalstore.correos.dead',
} as const;
