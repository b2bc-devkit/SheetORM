# SheetORM — Raport analityczny

## 1. Przeanalizowane artefakty Maven (najnowsze wersje)

| Biblioteka | Wersja | URL |
|---|---|---|
| dari-db | 5.0.27000972-x0f40c1 | https://artifactory.psdops.com/artifactory/psddev-releases/com/psddev/dari-db/5.0.27000972-x0f40c1/ |
| dari-mysql | 5.0.27000110-x27542b | https://artifactory.psdops.com/artifactory/psddev-releases/com/psddev/dari-mysql/5.0.27000110-x27542b/ |
| dari-sql | 5.0.27000148-x27542b | https://artifactory.psdops.com/artifactory/psddev-releases/com/psddev/dari-sql/5.0.27000148-x27542b/ |
| dari-util | 5.0.27000746-x3b4a1c | https://artifactory.psdops.com/artifactory/psddev-releases/com/psddev/dari-util/5.0.27000746-x3b4a1c/ |

## 2. Analiza licencyjna

### Wyniki
Żaden z przeanalizowanych POM-ów (v5.0.x) **nie zawiera sekcji `<license>`**. Artefakty publikowane są
w prywatnym repozytorium Maven firmy Perfect Sense (właściciel Brightspot CMS).

| Artefakt | Licencja w POM | Licencja efektywna | Ryzyko |
|---|---|---|---|
| dari-db 5.0 | brak | **Proprietary / Commercial** (Perfect Sense) | BLOCKER: unknown/proprietary license — NIE kopiować kodu |
| dari-mysql 5.0 | brak | **Proprietary / Commercial** | BLOCKER: j.w. |
| dari-sql 5.0 | brak | **Proprietary / Commercial** | BLOCKER: j.w. |
| dari-util 5.0 | brak | **Proprietary / Commercial** | BLOCKER: j.w. |

> **Uwaga historyczna**: Wcześniejsze wersje Dari (2.x) były publikowane jako open-source (Apache-2.0
> na GitHubie: perfectsense/dari). Od wersji 3.x artefakty przeniesiono do prywatnego repo
> `artifactory.psdops.com` bez jawnej licencji open-source, co wskazuje na licencję komercyjną/proprietary.

### Konsekwencje prawne
- **NIE kopiujemy żadnego kodu** z artefaktów Dari 5.x.
- Inspirujemy się wyłącznie **publiczną dokumentacją Brightspot** (docs.brightspot.com) oraz
  **wzorcami architektonicznymi** (Record pattern, Query builder, Index tables), które są ogólnodostępnymi
  wzorcami projektowymi (nie podlegają ochronie prawnoautorskiej).
- **Nazewnictwo**: nie używamy nazw „Dari", „Brightspot", „psddev" w publicznym API.

## 3. Kluczowe koncepcje z Dari/Brightspot → mapowanie na SheetORM

| Koncept Dari/Brightspot | Opis | Mapowanie SheetORM |
|---|---|---|
| `Record` | Bazowa klasa modelu, schemaless JSON | `Entity` — interfejs z `id` i polami |
| `ObjectType` | Metadata typu (pola, indeksy) | `TableSchema` — definicja schematu encji |
| `State` | Stan obiektu (dirty tracking, metadata) | Wewnętrzny tracking w `EntityStore` |
| `Database` | Warstwa persystencji | `SheetRepository<T>` — adapter Google Sheets |
| `Query.from().where()` | Fluent query builder | `QueryBuilder<T>` — where/order/limit/offset |
| `@Indexed` | Deklaracja indeksu na polu | `@Index` dekorator + `IndexStore` runtime |
| Transakcje (`beginWrites/commitWrites`) | Atomic batch | `BatchOperation` — buforowanie + `flush()` |
| `PaginatedResult` | Paginacja wyników | `PaginatedResult<T>` z offset/limit |
| `Grouping` | Agregacja po polu | `groupBy()` w QueryBuilder |
| Relacje referencyjne | UUID reference | Referencje po `id` między arkuszami |
| `@Embedded` | Obiekt zagnieżdżony | JSON w kolumnie lub sub-fields |
| `save/delete` lifecycle | Callbacki before/after | Hooki `beforeSave`, `afterSave`, `beforeDelete`, `afterDelete` |

## 4. Wzorce architektoniczne — rekomendacje

### 4.1 Serializacja
- Każda encja = 1 wiersz w arkuszu; kolumny = pola encji.
- Kolumny systemowe: `__id` (UUID), `__createdAt`, `__updatedAt`.
- Typy złożone (obiekty, tablice) serializowane jako JSON w kolumnie.

### 4.2 Indeksowanie
- Primary index: kolumna `__id` (UUID v4).
- Secondary indexes: oddzielne arkusze `_idx_{table}_{field}` z kolumnami `value` + `ids` (lista UUID).
- Unique index: walidacja unikalności przy save.
- Rebuild index: pełne przebudowanie indeksu z danych tabeli.

### 4.3 Query Builder
- Fluent API: `repo.query().where('email', '=', 'x').orderBy('name', 'asc').limit(10).execute()`.
- Predykaty: =, !=, <, >, <=, >=, contains, startsWith, in.
- Wykorzystanie indeksów when available, fallback na full scan.

### 4.4 Cache
- In-memory cache mapy id→rowIndex z TTL.
- Sheet data cache (pełne dane arkusza) z invalidation przy write.
- Batch reads: `Range.getValues()` zamiast cell-by-cell.

### 4.5 Batch Operations
- Grupowanie writes do `Range.setValues()`.
- `beginBatch()` / `commitBatch()` API.
- Auto-flush po przekroczeniu buffer size.

## 5. Ryzyka licencyjne — podsumowanie

| Ryzyko | Opis | Mitygacja |
|---|---|---|
| Brak licencji w POM | Artefakty 5.x nie deklarują licencji | NIE kopiujemy kodu; inspiracja wyłącznie wzorcami |
| Znaki towarowe | „Dari", „Brightspot" to nazwy produktów | Nie używamy w API/README; stosujemy „SheetORM" |
| Wzorce architektoniczne | Record/Query/Index to ogólne wzorce | Dozwolone — nie podlegają ochronie IP |
| Dokumentacja publiczna | docs.brightspot.com jest publicznie dostępna | Można czytać i inspirować się koncepcjami |

## 6. Klasy/metody z Dari jako inspiracja (referencje, bez kodu)

### Z dokumentacji Brightspot:
- **Record** → `save()`, `delete()`, `getState()` — inspiracja lifecycle CRUD
- **Query** → `from()`, `where()`, `sortAscending()`, `selectAll()`, `first()`, `select(offset,limit)`, `groupBy()` — fluent query API
- **Database** → `beginWrites()`, `commitWrites()`, `endWrites()` — transaction pattern
- **PaginatedResult** → `getItems()`, `getCount()`, `hasNext()`, `getNextOffset()` — pagination
- **Grouping** → `getKeys()`, `getCount()`, `createItemsQuery()` — aggregation
- **PredicateParser** → string-based predicate parsing — inspiracja parsera warunków
- **AsyncDatabaseReader/Writer** → pipeline async processing (poza scope MVP)

## 7. Rekomendacje architektoniczne

1. **Modularna architektura**: core (Entity, Schema) → storage (SheetAdapter) → query (QueryBuilder) → index (IndexStore)
2. **Dependency Injection**: interfejsy `ISpreadsheetAdapter`, `ICacheProvider` pozwalają na mockowanie
3. **Adapter pattern**: `GoogleSheetsAdapter` implementuje `ISpreadsheetAdapter`; łatwa zamiana na inne storage
4. **Schema-first**: definicja schematu (pola + typy + indeksy) przed pierwszym użyciem
5. **Lazy loading**: indeksy budowane on-demand; dane cachowane z TTL
6. **Batch-first**: domyślnie operacje batch; single-row jako special case batch(1)

## 8. Checklist implementacyjna

- [x] Raport analityczny
- [x] Specyfikacja API (SPEC.md)
- [x] Core: Entity, TableSchema, FieldDefinition
- [x] Storage: ISpreadsheetAdapter, GoogleSheetsAdapter
- [x] Repository: SheetRepository (CRUD)
- [x] Index: IndexStore (create/lookup/rebuild)
- [x] Query: QueryBuilder (where/order/limit/offset)
- [x] Cache: MemoryCache z TTL
- [x] Batch: BatchOperation (begin/commit)
- [x] Utils: UUID, serialization
- [x] Schematy arkuszy (schemas/)
- [x] Testy jednostkowe (mock)
- [x] Testy integracyjne (przykład)
- [x] Benchmark plan
- [x] CI workflow
- [x] README z instrukcjami
