# ADR-0001: Organizar o lakehouse em um catálogo `core` alinhado à fonte e catálogos por domínio para a gold

- **Status:** aceito — emendado pelo [ADR-0004](0004-ajustes-de-organizacao-nomes-landing-e-pii.md) (nomes das dimensões, prefixo na silver, layout da landing, PII) e pelo [ADR-0007](0007-retencao-da-bronze-por-tipo-de-extracao.md) (retenção)
- **Data:** 2026-10-16
- **Fatia:** fase 0
- **Decisor:** Engenheiro de Dados Sênior · **Revisor:** Staff Engineer

## Contexto

O lakehouse vai atender cinco domínios (comercial, supply, trade marketing, logística, data science) que leem fontes em comum: o ERP alimenta o comercial e a logística, e produto/PDV aparecem em todos. Precisávamos de uma organização que:

1. ingira cada fonte uma única vez;
2. mantenha uma definição só para as entidades compartilhadas (sem "dois números para a mesma pergunta");
3. permita dar acesso por domínio (ex.: Trade Marketing vê sell-out, não vê faturamento);
4. isole a PII exigida pela LGPD.

Restrições do ambiente (Databricks Free Edition): um metastore só; catálogos novos só podem ser criados via SQL (`CREATE CATALOG`, em default storage), já que a API/CLI recusa; tags/ABAC **não** concedem `SELECT` em tabelas (as GRANT policies cobrem só models/services/skills).

## Decisão

**Até a silver, organizamos por fonte; da gold em diante, por domínio.**

    core                                ← dono: time técnico (plataforma)
      landing     volumes por fonte (sellout, erp, wms, tms...)
      bronze      tabelas por fonte, com prefixo do sistema: erp_invoice, erp_orders, wms_stock_snapshot...
      silver      entidades limpas e deduplicadas, ainda alinhadas à fonte
      gold        SOMENTE dimensões conformadas: dim_product, dim_pos, dim_date, dim_sales_hierarchy
    commercial                          ← dono: comercial
      gold        fact_sales, fact_orders, agregados do comercial
    supply / trade_marketing / logistics / data_science
      gold        criados quando a fatia do domínio chegar

Regras:

- **Fatos** pertencem ao domínio que produz o evento (`commercial.gold.fact_orders`). Os demais domínios leem entre catálogos e **nunca reconstroem** o fato. Métrica que cruza processos (ex.: OTIF) pertence ao domínio que precisa dela.
- **Dimensões conformadas**: uma por entidade, em `core.gold`, com dono técnico único.
  - Mudança aditiva (nova coluna): o dono aprova e avisa os consumidores.
  - Quebra de contrato (renomear/remover coluna, mudar grão ou regra de SCD): dono + domínios consumidores aprovam, com versão e janela de depreciação.
  - Cada atributo tem um dono de origem responsável pela qualidade. Atributo estável do produto entra na dimensão; atributo volátil ou de um único processo vai para o fato. Tabela de extensão por domínio (`dim_product_ext_supply`) só como exceção.
- **`core.gold` contém apenas dimensões conformadas.** Qualquer outra tabela gold precisa de um domínio dono.
- **Nomes**: inglês, snake_case. Sem prefixo de camada (o schema já diz). Prefixo de **fonte** na bronze (`erp_`, `wms_`, `tms_`, `sellout_`). Sem prefixo de domínio (o catálogo já diz). Fatos `fact_*`, dimensões `dim_*`.
- **Landing**: um volume por fonte em `core.landing`; arquivos identificados por remetente e data, ex.: `/Volumes/core/landing/sellout/<data>/<remetente>-sellout-AAAAMMDD.csv`. O layout detalhado (atrasos, reenvios) fica para a fatia 3.
- **Acesso por domínio**: `GRANT` no catálogo do domínio para o grupo do domínio, aproveitando a herança de privilégios do UC. A bronze do `core` (com PII) tem acesso restrito ao time técnico; os domínios recebem `core.silver` e `core.gold` já mascarados.
- **Tags**: chave/valor (`domain = commercial`, `pii = true`) para descoberta e documentação. Não são mecanismo de concessão de acesso.
- **Ambientes**: nenhum. O workspace atual funciona como "prod" do projeto de estudos.
- **Criação**: na fase 0, apenas `core` e `commercial`.

## Alternativas consideradas

| Alternativa | Por que não |
|---|---|
| Catálogo único com schemas por camada (`serra_azul.bronze/silver/gold`) e domínio no nome ou em tags | Acesso por domínio exigiria um grant por tabela (tags não concedem `SELECT`); o risco de esquecer um grant cresce a cada tabela nova |
| Catálogo por domínio com bronze/silver/gold em cada um (ingestão duplicada) | Cada fonte seria ingerida várias vezes (custo dobrado na cota do Free Edition) e as cópias divergiriam, recriando na bronze o problema das definições conflitantes |
| Bronze no domínio dono do sistema (ERP → comercial, WMS → supply...) | Sem catálogo extra, mas as tabelas usadas por vários domínios cairiam nas costas de um único domínio de negócio, que não tem obrigação de atender os outros. No `core`, um time técnico é dono explícito da bronze como fonte da verdade |

## Consequências

- Positivas: ingestão única por fonte; uma definição por entidade compartilhada; acesso por domínio com um grant por catálogo; PII isolada num único ponto (bronze do `core`); caso das golds homônimas resolvido pelo catálogo (`commercial.gold.monthly_sales` ≠ `trade_marketing.gold.monthly_sales`).
- Negativas / dívidas assumidas: todos os domínios dependem do `core` e do time técnico dono dele (gargalo potencial); "toda a gold para um analista" vira um grant por catálogo de domínio; consultas entre domínios usam nomes de três partes; sem dev/prod, toda mudança é testada direto no "prod".
- O que nos faria revisitar: o time do `core` virar gargalo para mudanças de domínio; surgir uma silver específica de um único domínio que não faça sentido no `core`; o número de domínios crescer a ponto de os grants por catálogo ficarem difíceis de auditar.

## Em produção faríamos diferente?

Sim, em três pontos:

- **Ambientes.** Haveria dev, (staging) e prod. Como o metastore é um só por região, a separação seria por catálogo: ou o ambiente vira parte do nome (`core_dev`, `core_prod`, `commercial_dev`...), ou cada ambiente tem o próprio workspace e os catálogos são vinculados a ele com *workspace-catalog binding*, impedindo que um job de dev escreva em prod. A promoção seria por CI/CD com bundles (um target por ambiente) e os nomes de catálogo viriam de configuração, nunca do código (tema do ADR-0002).
- **Acesso.** Grants iriam para grupos sincronizados do IdP (SCIM), nunca para usuários. Mascaramento de PII por tag (políticas ABAC de column mask) seria avaliado; aqui sua disponibilidade no Free Edition ainda não foi verificada (fatia 1).
- **Retenção.** A bronze como fonte da verdade exige retenção compatível com o reprocessamento (e com a guarda fiscal de 7 anos). No projeto a bronze tem 90 dias — conflito registrado para um ADR próprio na fatia 1.

Em 10x de volume a organização se mantém (o custo está nos jobs, não nos nomes). Em 100x de domínios/times, o `core` centralizado tende a virar gargalo e seria preciso discutir dar mais autonomia de silver aos domínios, com contratos de dados entre eles.

## Conceitos para estudar

Namespace e herança de privilégios do Unity Catalog; dados alinhados à fonte vs ao consumo (data mesh); dimensões conformadas e bus matrix (Kimball); fatos por processo de negócio; quebra de contrato vs mudança aditiva; isolamento de ambientes e workspace-catalog binding; ABAC (row filters, column masks) e seus limites.
