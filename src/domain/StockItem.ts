/**
 * Saldo de uma peça no estoque da execução.
 * `available` pode ser reservado; `reserved` está preso a execuções ativas até a baixa (finalização)
 * ou a liberação (falha/cancelamento).
 */
export interface StockItem {
  partId: string;
  description: string | null;
  available: number;
  reserved: number;
  updatedAt: Date;
}

export interface MissingPart {
  partId: string;
  requested: number;
  available: number;
}
