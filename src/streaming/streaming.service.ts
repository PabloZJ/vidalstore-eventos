import { Injectable, OnModuleInit, OnModuleDestroy, Logger } from '@nestjs/common';
import { MessagesStreamModes, MessagesStreamFallbackModes } from '@platformatic/kafka';
import { consumidor } from './consumidor.js';
import { TOPICS } from './topics.js';

@Injectable()
export class StreamingService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(StreamingService.name);
  private flujo: any;

  async onModuleInit() {
    this.logger.log('Iniciando consumidor Kafka grupo vidalstore-panel...');
    this.flujo = await consumidor.consume({
      topics: [TOPICS.COMPRAS, TOPICS.ACTIVIDAD],
      autocommit: false,
      mode: MessagesStreamModes.COMMITTED,
      fallbackMode: MessagesStreamFallbackModes.EARLIEST,
    });
    void this.leer(); // sin await: si no, Nest no termina de arrancar
    this.logger.log('✓ Consumidor Kafka iniciado');
  }

  private async leer() {
    this.logger.log('Leyendo mensajes de Kafka...');
    for await (const m of this.flujo) {
      this.registrar(m);
      await m.commit();
    }
  }

  private registrar(mensaje: any) {
    try {
      const evento = JSON.parse(mensaje.value);
      this.logger.log(
        `[${mensaje.topic}] P${mensaje.partition} offset ${mensaje.offset}: ${evento.tipo || 'sin tipo'} (key=${mensaje.key})`,
      );
    } catch (error) {
      this.logger.warn(
        `[${mensaje.topic}] P${mensaje.partition} offset ${mensaje.offset}: JSON inválido - ${error instanceof Error ? error.message : 'error desconocido'}`,
      );
    }
  }

  async onModuleDestroy() {
    this.logger.log('Cerrando consumidor Kafka...');
    if (this.flujo) {
      await this.flujo.close();
    }
    await consumidor.close();
    this.logger.log('✓ Consumidor Kafka cerrado');
  }
}
