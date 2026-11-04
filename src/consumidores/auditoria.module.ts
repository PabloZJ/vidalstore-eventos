import { Module } from '@nestjs/common';
import { DatabaseModule } from '../database/database.module.js';
import { MensajeriaModule } from '../mensajeria/mensajeria.module.js';
import { AuditoriaConsumidor } from './auditoria.consumidor.js';

@Module({ imports: [DatabaseModule, MensajeriaModule], providers: [AuditoriaConsumidor] })
export class AuditoriaModule {}
