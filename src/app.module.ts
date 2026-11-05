import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { AuditoriaModule } from './consumidores/auditoria.module.js';
import { AvisosModule } from './consumidores/avisos.module.js';
import { CartasMuertasModule } from './consumidores/cartas-muertas.module.js';
import { CorreosModule } from './consumidores/correos.module.js';
import { DatabaseModule } from './database/database.module.js';
import { HealthModule } from './health/health.module.js';
import { MensajeriaModule } from './mensajeria/mensajeria.module.js';
import { StreamingModule } from './streaming/streaming.module.js';

@Module({
  imports: [
    ConfigModule.forRoot({ isGlobal: true }),
    DatabaseModule,
    HealthModule,
    MensajeriaModule,
    AuditoriaModule,
    AvisosModule,
    CorreosModule,
    CartasMuertasModule,
    StreamingModule,
  ],
})
export class AppModule {}
