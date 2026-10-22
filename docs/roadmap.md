# Roadmap — fatias verticais por domínio

Contexto completo em [caso-de-negocio.md](caso-de-negocio.md). Cada fatia atravessa fonte → bronze → silver → gold → consumo e termina com algo visível. Decisões importantes viram ADR em `docs/adr/`.

## Status geral

| Etapa | Entregável visível | Status |
|---|---|---|
| Fase 0 — Fundação | Esqueleto do lakehouse + convenções | ✅ concluída — ADR-0001/0002, catálogos `core`/`commercial` criados, [convenções](convencoes.md); revisada em 2026-10-19 (ADR-0004/0005) |
| Fatia 1 — Comercial | Dashboard de vendas por região/marca | 🟨 em andamento — modelagem fechada (ADR-0003, 0006), retenção (ADR-0007), carga inicial do ERP na landing ([dicionário](dicionario-de-dados-erp.md)), pipeline `core` com bronze, silver e dimensões (ADR-0008, 0009), pipeline `commercial` com os fatos (ADR-0010); próximo: job `serra_azul_daily` (com a prova de idempotência e a tarefa de verificação de unicidade), depois dashboard |
| Fatia 2 — Supply chain | Cobertura de estoque por CD | ⬜ não iniciada |
| Fatia 3 — Sell-out | Sell-in vs sell-out | ⬜ não iniciada |
| Fatia 4 — Logística | OTIF | ⬜ não iniciada |
| Fatia 5 — Data science | Tabela de features para forecast | ⬜ não iniciada |
| Post-mortem | Lições aprendidas | ⬜ não iniciada |

> Convenção de status: ⬜ não iniciada · 🟨 em andamento · ✅ concluída (critério de pronto atendido)
>
> Desde 2026-10-19, o Claude atua como Engenheiro Sênior que decide e implementa, e o usuário acompanha para aprender (ver `CLAUDE.md`). A [revisão de 2026-10-19](revisao-2026-10-19.md) relê as decisões anteriores.

---

## Fase 0 — Fundação (curta!)

**Objetivo:** o mínimo para a fatia 1 começar. Nada de framework genérico antes de existir o primeiro caso de uso (YAGNI).

- **Escopo:** discovery dos requisitos da fatia 1; convenções de nomenclatura; estrutura de catálogo/schemas (camadas bronze/silver/gold); volume(s) para arquivos de entrada; decisão de ferramenta de transformação (pipeline SDP vs jobs+notebooks — lembrar: **1 pipeline ativo por tipo** no Free Edition); template de ADR em uso.
- **Decisões a tomar** (todas tomadas; ver ADR-0001/0002 e a revisão nos ADR-0004/0005):
  - Um schema por camada ou por domínio? Onde entra o domínio no nome?
  - Quantos catálogos e com que critério (o Free Edition permite criar catálogos via SQL em *default storage*, mas há 1 metastore só) — como isso mudaria com múltiplos ambientes em produção?
  - SDP ou notebooks+jobs como motor das transformações? Qual o custo de trocar depois?
  - Como os dados fake entram: gerados direto em tabelas bronze ou como arquivos em volume (simulando landing zone)?
- **Conceitos de entrevista:** medallion architecture, data lake vs lakehouse, naming conventions, ambientes (dev/prod) e promoção de código.
- **Limitações do Free Edition:** 1 metastore/workspace; sem external locations; catálogos novos só via SQL (`CREATE CATALOG`, default storage); 1 pipeline ativo por tipo.
- **Critério de pronto:** schemas criados; convenções escritas em 1 página; ≥2 ADRs registrados (estrutura de catálogo; motor de transformação); dados fake da fatia 1 definidos (não necessariamente gerados).

---

## Fatia 1 — Comercial: vendas e faturamento

- **Pergunta de negócio:** "Quanto vendi ontem, por região, marca e canal?" — **Diretor Comercial**, SLA 8h.
- **Fontes:** ERP — pedidos (cab/itens), NF, clientes/PDV, produtos, tabela de preço, hierarquia comercial.
- **Entregável visível:** dashboard de vendas por região/marca/canal sobre a gold.
- **Decisões a tomar:**
  - ✅ star schema, fatos e grão: dois fatos no grão do item, pedido e NF (ADR-0003);
  - ✅ surrogate keys ou chaves naturais: as duas — `_key` determinística da versão + `_id` durável (ADR-0006);
  - ✅ SCD tipo 2 na hierarquia comercial, a partir da vigência do ERP (ADR-0006);
  - ✅ FKs órfãs: membro desconhecido `-1`, corrigido na recomputação (ADR-0006); motivo do `-1` exposto nos fatos (ADR-0010);
  - ✅ PII do cadastro: não sai da bronze (ADR-0004);
  - ✅ retenção da bronze (ADR-0007);
  - ✅ duplicatas de pedido: dedup na silver com `AUTO CDC` SCD 1, com desempate pelo ciclo de vida do pedido; regras de qualidade e suas ações (ADR-0008);
  - ✅ chaves e medidas dos fatos: vendedor do item de NF na data do pedido e cliente na data da NF, sinal das devoluções só no fato, bruto = líquido + desconto, `volume_hl` em `DECIMAL(18,5)` (ADR-0010);
  - ✅ vigências sobrepostas e buracos nos cadastros: medidos por regras de aviso na silver (ADR-0010);
  - ✅ toda expectation dá `true` ou `false`, nunca `NULL` (ADR-0010);
  - ✅ atributos tipo 1 da `dim_customer` pela última versão de cada cliente, e não pela versão aberta, como na `dim_product` (ADR-0011).
- **Ordem de implementação:** dados sintéticos na landing → bundle + pipeline `core` → pipeline `commercial` → job → dashboard → critérios de pronto (`done-checker`) → entrevista (`interviewer`).
- **Conceitos de entrevista:** Kimball (fato × dimensão, grão), SCD 1 vs 2, idempotência de carga incremental, MERGE vs append, liquid clustering vs particionamento.
- **Tema transversal em profundidade:** **qualidade de dados básica** — expectations/testes nas regras óbvias (valores negativos, FKs, duplicatas) + mascaramento de PII.
- **Limitações do Free Edition:** warehouse único 2X-Small para o dashboard; 1 pipeline SDP ativo.
- **Critério de pronto:** dashboard responde a pergunta com D-1; recarga do mesmo dia não duplica; SCD2 da hierarquia auditável (consulta "quem era o supervisor do vendedor X em tal data" funciona); ADRs de modelagem registrados; 2–3 perguntas de entrevista respondidas e avaliadas.

---

## Fatia 2 — Supply chain: estoque e produção

- **Pergunta de negócio:** "Qual a cobertura de estoque por CD e SKU? Onde vou romper?" — **Gerente de Supply**.
- **Fontes:** snapshot diário de estoque (WMS), apontamentos de produção (MES) + demanda média vinda da fatia 1.
- **Entregável visível:** visão de cobertura de estoque (dias de estoque) por CD × SKU.
- **Decisões a tomar:** fato de **snapshot** vs fato **transacional** — quando cada um; como tratar dias faltantes do snapshot (gap = zero? repete último? marca como desconhecido?); granularidades diferentes entre produção (por OP) e estoque (por dia); correção do CD que mandou caixas em vez de unidades; estoque negativo — corrigir, rejeitar ou expor?; schema técnico para o event log dos pipelines (hoje aparece em `core.bronze` e `commercial.gold`, legível por todo consumidor do catálogo; destino e configuração a verificar; dívida do ADR-0010).
- **Conceitos de entrevista:** periodic snapshot fact table, semi-aditividade (estoque não soma no tempo!), acumulating snapshot, tratamento de late data em snapshots.
- **Tema transversal em profundidade:** **observabilidade** — detectar fonte que não chegou (freshness), alertar gap de snapshot, monitorar contagens por dia.
- **Limitações do Free Edition:** alertas nativos limitados ao warehouse único; sem integrações externas de notificação garantidas.
- **Critério de pronto:** cobertura calculada e visível; dia sem snapshot é detectado e sinalizado (não silenciosamente zerado); documentada a regra de semi-aditividade no gold.

---

## Fatia 3 — Sell-out de distribuidores (a fatia suja)

- **Pergunta de negócio:** "Sell-in vs sell-out por região e marca — estou empurrando estoque para o canal?" — **Trade Marketing**.
- **Fontes:** 14 arquivos heterogêneos de sell-out + gold de vendas da fatia 1 (sell-in).
- **Entregável visível:** comparativo sell-in vs sell-out por região/marca/mês.
- **Decisões a tomar:** **contrato de dados** por fonte (o que é obrigatório? o que quarentena?); arquitetura de quarentena (tabela separada? coluna de status? dead-letter?); idempotência com **reenvio de mês inteiro** (substituir partição? MERGE? versão por arquivo?); schema evolution da coluna nova (`promo_flag`); de-para de SKU incompleto — descartar, quarentenar ou publicar "não mapeado"?; rastreabilidade: de qual arquivo veio cada linha.
- **Conceitos de entrevista:** data contracts, schema evolution/enforcement, quarentena e dead-letter queue, idempotência e reprocessamento, late-arriving facts, lineage em nível de arquivo (`_metadata.file_path`).
- **Tema transversal em profundidade:** **contratos de dados + reprocessamento** — é aqui que o projeto sobe de nível.
- **Limitações do Free Edition:** sem SFTP real — a chegada de arquivos é simulada em volume UC; file arrival trigger a validar no ambiente.
- **Critério de pronto:** reenvio de um mês não duplica nem perde dados; registro inválido vai para quarentena com motivo; coluna nova absorvida sem quebrar; relatório sell-in × sell-out fecha com os dados de teste.

---

## Fatia 4 — Logística: entregas e devoluções

- **Pergunta de negócio:** "Qual meu OTIF (on time, in full) por CD e transportadora?" — **Supply + Comercial**.
- **Fontes:** TMS (entregas, devoluções) + fato de pedidos da fatia 1.
- **Entregável visível:** OTIF por CD/região/mês.
- **Decisões a tomar:** o join pedido→entrega sustenta a modelagem da fatia 1 ou expõe erro de grão? (teste real da arquitetura); entregas órfãs e timestamps invertidos — regra de negócio ou erro?; skew regional no join (SE = 45% do volume); definição exata de "on time" e "in full" com o negócio (quem define? onde documenta?); liquid clustering em materialized view (suporte a verificar), *skew* das 2 redes (26,2% do volume) na junção com o cliente e refresh incremental dos fatos comerciais (dívidas do ADR-0010).
- **Conceitos de entrevista:** fact-to-fact join (e por que evitar), conformed dimensions, skew e salting/broadcast, métricas compostas e sua governança (metric views?).
- **Tema transversal em profundidade:** **performance e custo** — plano de query, liquid clustering nas tabelas grandes, medir antes/depois.
- **Limitações do Free Edition:** sem controle de cluster, a otimização é toda em layout de dados e SQL; medição de custo limitada às system tables.
- **Critério de pronto:** OTIF publicado e batendo com casos de teste manuais; pelo menos 1 otimização medida (antes/depois documentado em ADR); modelagem da fatia 1 revisitada e veredito registrado.

---

## Fatia 5 — Data science: base para previsão de demanda

- **Pergunta de negócio:** "Features semanais por SKU × CD, prontas para um modelo de forecast" — **Data Science**.
- **Fontes:** todas as anteriores + clima + feriados.
- **Entregável visível:** tabela de features (`features_demanda_semanal`) documentada, com dicionário de dados.
- **Decisões a tomar:** grão semanal — como agregar sem **vazamento de dados futuros** (lags, janelas); calendário fiscal vs civil; como integrar clima (cidade do CD? do PDV?); feriados regionais no grão certo; features de sell-out valem a pena dado o atraso estrutural da fonte?; point-in-time correctness com as SCD2.
- **Conceitos de entrevista:** feature engineering em batch, data leakage, point-in-time joins, train/serve skew, qualidade como pré-requisito de ML.
- **Tema transversal em profundidade:** **governança e documentação** — dicionário de dados, lineage ponta a ponta, tags de PII revisadas, "este dado é confiável para treinar?".
- **Limitações do Free Edition:** acesso de rede restrito pode impedir chamar APIs reais de clima → dados externos possivelmente fake (registrar em ADR).
- **Critério de pronto:** tabela de features reproduzível para qualquer semana passada (point-in-time); dicionário completo; checagem anti-leakage documentada.

---

## Post-mortem

- O que cada fatia ensinou; decisões que se sustentaram vs as que foram revisitadas (com links para os ADRs).
- Consolidação do "em produção faríamos diferente" de todos os ADRs.
- O que ficou de fora e por quê (streaming: quando passaria a fazer sentido neste cenário?).
- Autoavaliação contra um loop de entrevista real de Senior DE: o que eu responderia melhor agora?
