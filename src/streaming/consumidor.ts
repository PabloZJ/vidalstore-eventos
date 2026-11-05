import {
  Consumer,
  MessagesStreamModes,
  MessagesStreamFallbackModes,
  stringDeserializers,
} from '@platformatic/kafka';

export const consumidor = new Consumer({
  groupId: 'vidalstore-panel',
  clientId: 'vidalstore-eventos',
  bootstrapBrokers: process.env.KAFKA_BROKERS!.split(','),
  deserializers: stringDeserializers,
});
