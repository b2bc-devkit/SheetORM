// SheetORM — Test fixtures: sample schemas and data for testing

import { TableSchema, Entity } from '../src/core/types';

// ─── User Schema ─────────────────────────────────────

export interface User extends Entity {
  name: string;
  email: string;
  age: number;
  active: boolean;
}

export const userSchema: TableSchema = {
  tableName: 'Users',
  fields: [
    { name: 'name', type: 'string', required: true },
    { name: 'email', type: 'string', required: true },
    { name: 'age', type: 'number' },
    { name: 'active', type: 'boolean', defaultValue: true },
  ],
  indexes: [
    { field: 'email', unique: true },
    { field: 'name' },
  ],
};

// ─── Order Schema ────────────────────────────────────

export interface Order extends Entity {
  userId: string;
  product: string;
  quantity: number;
  total: number;
  status: string;
}

export const orderSchema: TableSchema = {
  tableName: 'Orders',
  fields: [
    { name: 'userId', type: 'reference', required: true, referenceTable: 'Users' },
    { name: 'product', type: 'string', required: true },
    { name: 'quantity', type: 'number', required: true },
    { name: 'total', type: 'number', required: true },
    { name: 'status', type: 'string', defaultValue: 'pending' },
  ],
  indexes: [
    { field: 'userId' },
    { field: 'status' },
  ],
};

// ─── Sample Data ─────────────────────────────────────

export const sampleUsers: User[] = [
  {
    __id: 'user-001',
    __createdAt: '2024-01-15T10:00:00.000Z',
    __updatedAt: '2024-01-15T10:00:00.000Z',
    name: 'Jan Kowalski',
    email: 'jan@example.com',
    age: 30,
    active: true,
  },
  {
    __id: 'user-002',
    __createdAt: '2024-01-15T11:00:00.000Z',
    __updatedAt: '2024-01-16T09:00:00.000Z',
    name: 'Anna Nowak',
    email: 'anna@example.com',
    age: 28,
    active: true,
  },
  {
    __id: 'user-003',
    __createdAt: '2024-01-17T08:00:00.000Z',
    __updatedAt: '2024-01-17T08:00:00.000Z',
    name: 'Piotr Wiśniewski',
    email: 'piotr@example.com',
    age: 45,
    active: false,
  },
];

export const sampleOrders: Order[] = [
  {
    __id: 'order-001',
    __createdAt: '2024-01-20T10:00:00.000Z',
    __updatedAt: '2024-01-20T10:00:00.000Z',
    userId: 'user-001',
    product: 'Widget A',
    quantity: 2,
    total: 49.98,
    status: 'completed',
  },
  {
    __id: 'order-002',
    __createdAt: '2024-01-21T14:00:00.000Z',
    __updatedAt: '2024-01-21T14:00:00.000Z',
    userId: 'user-002',
    product: 'Widget B',
    quantity: 1,
    total: 29.99,
    status: 'pending',
  },
];
