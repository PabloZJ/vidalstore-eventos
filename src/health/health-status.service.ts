import { Injectable } from '@nestjs/common';

@Injectable()
export class HealthStatusService {
  private rabbitOk = false;

  setRabbitOk(value: boolean): void {
    this.rabbitOk = value;
  }

  isRabbitOk(): boolean {
    return this.rabbitOk;
  }
}
