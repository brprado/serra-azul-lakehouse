# Learner Profile

Learning mode: paused
Onboarding: complete

## Project
Situation: New
Building: Plataforma de dados (lakehouse medallion) da fictícia Cervejaria Serra Azul — projeto de estudos simulando trabalho real de Engenheiro de Dados Sênior, em role-play (learner = Sênior que decide; Claude = Staff que questiona). Contexto em docs/caso-de-negocio.md; plano em docs/roadmap.md; regras permanentes em CLAUDE.md.
Codebase familiarity: n/a (greenfield)
Learning scope: entire system

## Experience
Overall programming: Intermediate
Stack familiarity: Intermediate (Databricks, Spark, SQL, modelagem dimensional — média autodeclarada)

## Goals
Primary: Praticar o que é cobrado em entrevistas de Engenheiro de Dados Sênior: modelagem, arquitetura, trade-offs, otimização, qualidade de dados, governança e decisões justificadas (com ADRs).
Capability goal: Ao fim de cada fatia, responder 2–3 perguntas estilo entrevista e ser avaliado.

## Preferences
Checkpoint frequency: Light (escolhido em 2026-10-16 após feedback "você está complicando demais"; parar só em decisões grandes — modelagem/arquitetura; detalhes vão direto no Implementation checkpoint)
Question style: Open-ended
Implementation style: AI writes code
Role (desde 2026-10-19): Claude atua como Engenheiro Sênior que decide, implementa e justifica tudo; o learner acompanha, pergunta e pode vetar (pedido explícito; CLAUDE.md reescrito).

## Strong Concepts
- Dimensões conformadas e ownership de fatos por processo produtor (aplicou ao caso pedidos × logística).
- Mudança aditiva vs quebra de contrato em tabelas compartilhadas.
- Maturidade organizacional como pré-requisito de descentralização (data mesh) — resposta de entrevista da fase 0.

## Developing Concepts
- Namespace do Unity Catalog (errou formato de volume por 3 rodadas; corrigido).
- Justificar escolhas técnicas com mecanismos concretos em vez de preferência ("foi mais gosto").
- Recursos do SDP (AUTO CDC, SEQUENCE BY, TRACK HISTORY ON, expectations) — conheceu na fase 0, ainda não aplicou.
- Controle de acesso no UC (herança de privilégios; tags não concedem SELECT).
- SCD2: escolhe por atributo e já explica o join point-in-time (corrigiu a ideia de "filtrar a versão ativa"); ainda confunde versão da mesma entidade com nova entidade sob a mesma chave natural (SKU reaproveitado) — chave natural vs substituta em desenvolvimento.

## Revisit
- Bronze como fonte da verdade vs retenção de 90 dias (ADR próprio na fatia 1).
- Quando NÃO usar SDP: contraexemplos mais precisos (Lakeflow Connect para ERP/CRM; data sources Python custom).
