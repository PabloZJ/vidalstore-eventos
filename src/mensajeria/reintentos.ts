// Configuración de reintentos y clasificación de errores para mensajería.
export const REINTENTOS = { maximo: 3, esperaMs: 2000 } as const;

// Un error es del MENSAJE cuando reintentar no lo va a arreglar nunca.
export class MensajeInvalido extends Error {}

export function esDelMensaje(error: unknown): boolean {
  if (error instanceof SyntaxError || error instanceof MensajeInvalido) return true;
  const codigo = (error as { code?: unknown }).code;
  // SQLSTATE 22 = dato mal formado, 23 = restricción violada: está mal el dato, no la base.
  return typeof codigo === 'string' && (codigo.startsWith('22') || codigo.startsWith('23'));
}

const esperar = (ms: number) => new Promise<void>((listo) => setTimeout(listo, ms));

// Un AggregateError (localhost = ::1 y 127.0.0.1) trae message vacío; el code sí viene.
export function detalle(error: unknown): string {
  const e = error as { message?: string; code?: unknown };
  return e.message || String(e.code ?? error);
}

// El contador es la variable intento: vive en memoria mientras el mensaje sigue unacked.
export async function conReintentos<T>(
  trabajo: () => Promise<T>,
  alFallar: (intento: number, error: Error) => void,
): Promise<T> {
  for (let intento = 1; ; intento++) {
    try {
      return await trabajo();
    } catch (error) {
      if (esDelMensaje(error) || intento >= REINTENTOS.maximo) throw error;
      alFallar(intento, error as Error);
      await esperar(REINTENTOS.esperaMs * intento);
    }
  }
}
