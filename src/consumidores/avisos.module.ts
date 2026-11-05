import { Module } from '@nestjs/common';
import { DatabaseModule } from '../database/database.module.js';
import { MensajeriaModule } from '../mensajeria/mensajeria.module.js';
import { AvisosConsumidor } from './avisos.consumidor.js';

@Module({ imports: [DatabaseModule, MensajeriaModule], providers: [AvisosConsumidor] })
export class AvisosModule {}
