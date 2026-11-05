import { Module } from '@nestjs/common';
import { HealthController } from './health.controller.js';
import { HealthStatusService } from './health-status.service.js';

@Module({ controllers: [HealthController], providers: [HealthStatusService], exports: [HealthStatusService] })
export class HealthModule {}
