import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import pg from 'pg';

const { Pool } = pg;

@Injectable()
export class DatabaseService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(DatabaseService.name);
  private readonly pool = new Pool({
    host: process.env.DB_HOST || 'postgres',
    port: Number(process.env.DB_PORT) || 5432,
    user: process.env.DB_USER || 'vidalstore',
    password: process.env.DB_PASSWORD || 'vidalstore',
    database: process.env.DB_NAME || 'vidalstore',
  });

  async onModuleInit(): Promise<void> {
    await this.pool.query(`
      CREATE TABLE IF NOT EXISTS eventos_auditoria (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(), evento_id UUID UNIQUE NOT NULL,
        routing_key VARCHAR(100) NOT NULL, usuario_sub VARCHAR(200), payload JSONB NOT NULL,
        emitido_en TIMESTAMPTZ, recibido_en TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
      CREATE TABLE IF NOT EXISTS notificaciones (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(), para VARCHAR(200) NOT NULL,
        asunto TEXT NOT NULL, estado VARCHAR(20) NOT NULL DEFAULT 'enviada'
          CHECK (estado IN ('enviada', 'fallida')), evento_id UUID NOT NULL,
        creada_en TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
      CREATE UNIQUE INDEX IF NOT EXISTS notificaciones_evento_tipo_unico
        ON notificaciones (evento_id, asunto);
      CREATE TABLE IF NOT EXISTS mensajes_muertos (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(), cola_origen VARCHAR(100) NOT NULL,
        routing_key VARCHAR(100) NOT NULL, motivo TEXT, intentos INTEGER NOT NULL DEFAULT 1,
        payload TEXT NOT NULL, recibido_en TIMESTAMPTZ NOT NULL DEFAULT NOW()
      );
    `);
    this.logger.log('Tablas de vidalstore-eventos verificadas/creadas');
  }

  async onModuleDestroy(): Promise<void> {
    await this.pool.end();
  }

  query(text: string, values?: unknown[]) {
    return this.pool.query(text, values);
  }

  async isHealthy(): Promise<boolean> {
    try {
      await this.pool.query('SELECT 1');
      return true;
    } catch {
      return false;
    }
  }
}
