import { Injectable, Logger, OnApplicationBootstrap, OnModuleDestroy } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import amqp, { type ChannelModel, type ConfirmChannel } from 'amqplib';
import { HealthStatusService } from '../health/health-status.service.js';
import { declararTopologia } from './topologia.js';

@Injectable()
export class RabbitmqService implements OnApplicationBootstrap, OnModuleDestroy {
  private readonly logger = new Logger(RabbitmqService.name);
  private connection: ChannelModel | null = null;
  private reconnectTimer: NodeJS.Timeout | null = null;
  private closing = false;
  private channel: ConfirmChannel | null = null;
  private readonly inicializadores: Array<(channel: ConfirmChannel) => void> = [];

  constructor(private readonly healthStatus: HealthStatusService, private readonly config: ConfigService) {}

  registrarConsumidor(inicializador: (channel: ConfirmChannel) => void): void {
    this.inicializadores.push(inicializador);
    if (this.channel) inicializador(this.channel);
  }

  onApplicationBootstrap(): void {
    void this.conectar();
  }

  async onModuleDestroy(): Promise<void> {
    this.closing = true;
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    await this.connection?.close().catch(() => undefined);
  }

  private async conectar(): Promise<void> {
    const url = this.config.get<string>('RABBITMQ_URL') ?? 'amqp://vidalstore:vidalstore@rabbitmq:5672';
    try {
      const connection = await amqp.connect(url);
      const channel: ConfirmChannel = await connection.createConfirmChannel();
      channel.prefetch(1);
      await declararTopologia(channel);
      this.connection = connection;
      this.channel = channel;
      this.healthStatus.setRabbitOk(true);
      this.inicializadores.forEach((inicializador) => inicializador(channel));
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
    this.channel = null;
    if (this.closing || this.reconnectTimer) return;
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      void this.conectar();
    }, 5000);
  }
}
