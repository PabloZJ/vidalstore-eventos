import { Module } from '@nestjs/common';
import { DatabaseModule } from '../database/database.module.js';
import { MensajeriaModule } from '../mensajeria/mensajeria.module.js';
import { CorreosConsumidor } from './correos.consumidor.js';

@Module({ imports: [DatabaseModule, MensajeriaModule], providers: [CorreosConsumidor] })
export class CorreosModule {}
