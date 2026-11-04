import { Module } from '@nestjs/common';
import { DatabaseModule } from '../database/database.module.js';
import { MensajeriaModule } from '../mensajeria/mensajeria.module.js';
import { CartasMuertasConsumidor } from './cartas-muertas.consumidor.js';

@Module({ imports: [DatabaseModule, MensajeriaModule], providers: [CartasMuertasConsumidor] })
export class CartasMuertasModule {}
