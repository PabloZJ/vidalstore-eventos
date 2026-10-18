import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module.js';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);
  const port = process.env.PORT ? Number(process.env.PORT) : 3010;
  await app.listen(port);
  console.log(`vidalstore-eventos escuchando en el puerto ${port}`);
}

bootstrap();
