export interface Entity {
  __id: string;
  __createdAt?: string;
  __updatedAt?: string;
  [key: string]: unknown;
}
