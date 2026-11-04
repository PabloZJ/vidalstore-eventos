import { Module } from '@nestjs/common';
import { ConsumidoresService } from './consumidores/consumidores.service.js';
import { DatabaseService } from './database/database.service.js';
import { HealthController } from './health/health.controller.js';
import { HealthStatusService } from './health/health-status.service.js';
import { MensajeService } from './mensajeria/mensaje.service.js';
import { RabbitmqService } from './mensajeria/rabbitmq.service.js';

@Module({
  controllers: [HealthController],
  providers: [
    DatabaseService,
    HealthStatusService,
    MensajeService,
    ConsumidoresService,
    RabbitmqService,
  ],
})
export class AppModule {}
