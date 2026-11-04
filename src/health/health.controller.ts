import { Controller, Get } from '@nestjs/common';
import { DatabaseService } from '../database/database.service.js';
import { HealthStatusService } from './health-status.service.js';

@Controller()
export class HealthController {
  constructor(
    private readonly database: DatabaseService,
    private readonly healthStatus: HealthStatusService,
  ) {}

  @Get()
  async check() {
    const dbOk = await this.database.isHealthy();
    const rabbitOk = this.healthStatus.isRabbitOk();
    return {
      status: dbOk && rabbitOk ? 'ok' : 'degradado',
      service: 'vidalstore-eventos', db: dbOk, rabbit: rabbitOk,
      timestamp: new Date().toISOString(),
    };
  }

  @Get('salud')
  salud() {
    return this.check();
  }
}
