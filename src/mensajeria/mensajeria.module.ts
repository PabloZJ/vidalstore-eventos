import { Module } from '@nestjs/common';
import { HealthModule } from '../health/health.module.js';
import { MensajeService } from './mensaje.service.js';
import { RabbitmqService } from './rabbitmq.service.js';

@Module({ imports: [HealthModule], providers: [MensajeService, RabbitmqService], exports: [MensajeService, RabbitmqService] })
export class MensajeriaModule {}
