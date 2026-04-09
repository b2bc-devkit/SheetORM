# Security Policy

## Supported Versions

Only the latest published version of SheetORM receives security fixes.

| Version | Supported |
| ------- | --------- |
| latest  | ✅        |
| older   | ❌        |

## Reporting a Vulnerability

**Do not open a public GitHub issue for security vulnerabilities.**

Report security issues privately via GitHub's
[Security Advisories](https://github.com/b2bc-devkit/SheetORM/security/advisories/new) feature.

Include in your report:

- A clear description of the vulnerability
- Steps to reproduce or a proof-of-concept
- Potential impact and affected versions
- Any suggested mitigations (optional)

## Response Timeline

| Stage                       | Target   |
| --------------------------- | -------- |
| Acknowledgement             | 48 hours |
| Initial assessment          | 5 days   |
| Fix or mitigation published | 30 days  |

We will keep you informed throughout the process. If a CVE is warranted, we will handle the request.

## Scope

SheetORM is a TypeScript ORM library that runs inside **Google Apps Script**. The following areas are in scope
for security reports:

- Unsafe deserialization or injection via user-controlled field values
- Prototype pollution in record construction or query building
- Credential or token leakage through logging (`SheetOrmLogger`)
- Dependency vulnerabilities in published npm packages (`@b2bc-devkit/sheetorm`)

The following are **out of scope**:

- Security of the Google Apps Script or Google Sheets platform itself
- Issues that require physical access to the user's Google account
- Theoretical vulnerabilities without a realistic attack vector

## Disclosure Policy

We follow coordinated disclosure. Once a fix is published, details of the vulnerability will be shared in the
release notes and, if applicable, in a GitHub Security Advisory.
