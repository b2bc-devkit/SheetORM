# Schematy arkuszy SheetORM

## Konwencje

### Kolumny systemowe
Każdy arkusz encji zawiera 3 kolumny systemowe na początku:
- `__id` — UUID v4 (klucz główny)
- `__createdAt` — ISO 8601 timestamp utworzenia
- `__updatedAt` — ISO 8601 timestamp ostatniej modyfikacji

### Arkusz metadanych: `_meta`
| tableName | schemaJson | version |
|-----------|------------|---------|
| Users | `{"tableName":"Users","fields":[...],"indexes":[...]}` | 1 |
| Orders | `{"tableName":"Orders","fields":[...],"indexes":[...]}` | 1 |

### Arkusze indeksów: `_idx_{Table}_{field}`
| value | entityId |
|-------|----------|
| jan@example.com | a1b2c3d4-... |
| anna@example.com | e5f6g7h8-... |

---

## Przykład: schemat "Users"

### Arkusz `Users`
| __id | __createdAt | __updatedAt | name | email | age | active |
|------|-------------|-------------|------|-------|-----|--------|
| a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d | 2024-01-15T10:30:00.000Z | 2024-01-15T10:30:00.000Z | Jan Kowalski | jan@example.com | 30 | true |
| e5f6g7h8-i9j0-4k1l-2m3n-4o5p6q7r8s9t | 2024-01-15T11:00:00.000Z | 2024-01-16T09:15:00.000Z | Anna Nowak | anna@example.com | 28 | true |

### Arkusz `_idx_Users_email` (unique index)
| value | entityId |
|-------|----------|
| jan@example.com | a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d |
| anna@example.com | e5f6g7h8-i9j0-4k1l-2m3n-4o5p6q7r8s9t |

### Arkusz `_idx_Users_name`
| value | entityId |
|-------|----------|
| Jan Kowalski | a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d |
| Anna Nowak | e5f6g7h8-i9j0-4k1l-2m3n-4o5p6q7r8s9t |

---

## Przykład: schemat "Orders"

### Definicja TypeScript
```typescript
const orderSchema: TableSchema = {
  tableName: 'Orders',
  fields: [
    { name: 'userId', type: 'reference', required: true, referenceTable: 'Users' },
    { name: 'product', type: 'string', required: true },
    { name: 'quantity', type: 'number', required: true },
    { name: 'total', type: 'number', required: true },
    { name: 'status', type: 'string', defaultValue: 'pending' },
    { name: 'metadata', type: 'json' },
  ],
  indexes: [
    { field: 'userId' },
    { field: 'status' },
  ],
};
```

### Arkusz `Orders`
| __id | __createdAt | __updatedAt | userId | product | quantity | total | status | metadata |
|------|-------------|-------------|--------|---------|----------|-------|--------|----------|
| uuid-order-1 | 2024-01-20T... | 2024-01-20T... | a1b2c3d4-... | Widget A | 2 | 49.98 | completed | `{"priority":"high"}` |

### Arkusz `_idx_Orders_userId`
| value | entityId |
|-------|----------|
| a1b2c3d4-... | uuid-order-1 |

### Arkusz `_idx_Orders_status`
| value | entityId |
|-------|----------|
| completed | uuid-order-1 |
