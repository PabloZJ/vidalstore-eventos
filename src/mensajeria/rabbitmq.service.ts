import { Injectable, Logger, OnApplicationBootstrap, OnModuleDestroy } from '@nestjs/common';
import amqp, { type ChannelModel, type ConfirmChannel } from 'amqplib';
import { ConsumidoresService } from '../consumidores/consumidores.service.js';
import { HealthStatusService } from '../health/health-status.service.js';
import { declararTopologia } from './topologia.js';

@Injectable()
export class RabbitmqService implements OnApplicationBootstrap, OnModuleDestroy {
  private readonly logger = new Logger(RabbitmqService.name);
  private connection: ChannelModel | null = null;
  private reconnectTimer: NodeJS.Timeout | null = null;
  private closing = false;

  constructor(
    private readonly consumidores: ConsumidoresService,
    private readonly healthStatus: HealthStatusService,
  ) {}

  onApplicationBootstrap(): void {
    void this.conectar();
  }

  async onModuleDestroy(): Promise<void> {
    this.closing = true;
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    await this.connection?.close().catch(() => undefined);
  }

  private async conectar(): Promise<void> {
    const url = process.env.RABBITMQ_URL || 'amqp://vidalstore:vidalstore@rabbitmq:5672';
    try {
      const connection = await amqp.connect(url);
      const channel: ConfirmChannel = await connection.createConfirmChannel();
      channel.prefetch(1);
      await declararTopologia(channel);
      this.connection = connection;
      this.healthStatus.setRabbitOk(true);
      this.consumidores.iniciar(channel);
      this.logger.log('Topologia RabbitMQ configurada y consumidores iniciados');
      connection.on('error', () => this.healthStatus.setRabbitOk(false));
      connection.on('close', () => this.programarReconexion());
    } catch (error) {
      this.logger.error('Error conectando a RabbitMQ, reintentando en 5s...', error);
      this.programarReconexion();
    }
  }

  private programarReconexion(): void {
    this.healthStatus.setRabbitOk(false);
    if (this.closing || this.reconnectTimer) return;
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      void this.conectar();
    }, 5000);
  }
}
