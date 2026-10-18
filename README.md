# VidalStore Eventos

Microservicio NestJS encargado de consumir eventos de VidalStore, persistir auditoria y gestionar notificaciones mediante RabbitMQ y PostgreSQL.

## Responsabilidades

- Consumir eventos de dominio desde RabbitMQ.
- Persistir eventos recibidos en `eventos_auditoria`.
- Registrar notificaciones en `notificaciones`.
- Enviar mensajes fallidos a colas de mensajes muertos.
- Aplicar ACK/NACK y reintentos limitados.
- Exponer el estado de PostgreSQL y RabbitMQ.

## Puerto

El servicio escucha en el puerto `3010` por defecto. Puede modificarse mediante `PORT`.

## Endpoints

| Método | Ruta | Descripción |
| --- | --- | --- |
| GET | `/` | Estado de PostgreSQL y RabbitMQ |
| GET | `/salud` | Alias del endpoint de salud |

## Dependencias de infraestructura

- RabbitMQ en `RABBITMQ_URL`.
- PostgreSQL configurado mediante `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD` y `DB_NAME`.

Si no se especifican variables, se utilizan los valores predeterminados del entorno Docker Compose de VidalStore.

## Desarrollo

```bash
npm install
npm run start:dev
```

## Verificación

```bash
npm run build
npm run lint
npm test
```

## Docker

```bash
docker build -t vidalstore-eventos .
docker run --env-file .env -p 3010:3010 vidalstore-eventos
```

## Topologia

El servicio consume el exchange `vidalstore.eventos` y persiste los eventos recibidos en PostgreSQL. Las colas de auditoria, avisos y correos tienen sus respectivas colas de mensajes muertos para reintentos agotados.

## Estado del repositorio

La rama `main` contiene la version publicada del servicio. La rama `develop` se utiliza para integrar cambios antes de crear una release.
