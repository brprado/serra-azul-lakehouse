# Caso de negócio — Cervejaria Serra Azul

> Caso fictício para o projeto de estudos. Nenhuma empresa, marca ou pessoa real.
> Dados gerados com a skill `databricks-synthetic-data-gen` do Databricks AI Tools.

## 1. A empresa

A **Cervejaria Serra Azul** é uma fabricante de bebidas fundada em 1998 em Belo Horizonte/MG. Faturamento de ~R$ 2,8 bi/ano, **3 fábricas** (Contagem/MG, Itu/SP, Feira de Santana/BA) e **8 centros de distribuição (CDs)** cobrindo todas as regiões do país.

**Canais de venda:**
- **Distribuidores** (12 parceiros regionais) — ~55% do volume
- **Venda direta a redes varejistas** (2 redes nacionais de supermercados) — ~25%
- **Venda direta a PDVs** (bares, restaurantes, mercados de vizinhança) — ~20%, carteira de ~40 mil PDVs ativos

**Momento do negócio:** crescimento de ~18% a.a., puxado pela expansão no Nordeste e pelo lançamento da linha não alcoólica. Pressão de margem por custo de insumos (malte, alumínio) e frete. A diretoria quer substituir o ciclo atual — planilhas extraídas manualmente do ERP, consolidadas com 3–5 dias de atraso — por uma plataforma de dados. **Este projeto é o greenfield dessa plataforma.**

## 2. Portfólio

| Marca | Categoria | Posicionamento |
|---|---|---|
| Serra Azul Pilsen | Cerveja mainstream | Volume, guerra de preço |
| Serra Azul Puro Malte | Cerveja premium-mainstream | Margem |
| Alvorada | Cerveja artesanal (IPA, Weiss, APA) | Nicho, crescimento rápido |
| Névoa Zero | Cerveja sem álcool | Aposta de crescimento |
| Guará Serra | Refrigerante de guaraná | Diversificação, PET |

Embalagens: lata 269/350/473 ml, long neck 330 ml, garrafa retornável 600 ml, barril 30/50 L, PET 2 L. Combinações marca × líquido × embalagem × pack geram **~80 SKUs ativos** (92 produtos ao longo dos 18 meses, contando lançamentos, promoções e descontinuados).

## 3. Stakeholders e o que cada um precisa

| Stakeholder | Pergunta principal | Expectativa |
|---|---|---|
| **Diretor Comercial** | "Quanto vendi ontem, por região, marca e canal? Como estou contra a meta?" | Dados D-1 disponíveis **até 8h**; é o SLA mais duro do projeto |
| **Gerente de Supply Chain** | "Qual a cobertura de estoque por CD e SKU? Onde vou romper?" | Snapshot diário de estoque cruzado com demanda média |
| **Controladoria (CFO)** | "Faturamento, preço médio praticado e desconto por canal — batendo com o fiscal" | Precisão > velocidade; reconciliável com a contabilidade |
| **Trade Marketing** | "O que o distribuidor de fato vendeu ao PDV (sell-out)? Sell-in vs sell-out por região?" | Convive com dados atrasados e sujos; quer tendência confiável |
| **Data Science** | "Uma tabela de features confiável para prever demanda semanal por SKU × CD" | Histórico longo, sem vazamento de dados futuros, com clima e feriados |

## 4. Fontes de dados

| # | Fonte | Natureza | Entrega | Qualidade esperada |
|---|---|---|---|---|
| 1 | **ERP** (OLTP, "Protheus-like") | Pedidos (cabeçalho + itens), notas fiscais, clientes/PDVs, produtos, tabelas de preço, hierarquia comercial | Extração batch diária: incremental para fatos, full para dimensões | Melhor fonte da casa, mas com duplicatas de integração e cadastros imperfeitos |
| 2 | **MES/WMS** (produção e estoque) | Apontamentos de produção por fábrica; snapshot diário de estoque por CD × SKU | Arquivo diário por planta/CD | CDs esquecem de enviar; unidades de medida inconsistentes |
| 3 | **TMS** (logística) | Rotas, entregas com timestamps, devoluções com motivo | Batch diário | Timestamps fora de ordem, fusos, entregas órfãs |
| 4 | **Sell-out** (distribuidores e redes) | Vendas do distribuidor/varejista ao consumidor final | CSV/XLSX por SFTP ou e-mail, 14 remetentes, layouts distintos | **A pior fonte.** Atrasos de D+2 a D+7, reenvios completos, schema que muda sem aviso |
| 5 | **APIs externas** | Clima (temperatura/precipitação por cidade), feriados nacionais/estaduais/municipais | Chamada diária / carga anual | Gaps de medição |

## 5. Datasets fake: definição, volumes e sujeiras intencionais

Horizonte: **18 meses de história** para o ERP/estoque/logística e **12 meses** de sell-out. Volumes propositalmente modestos para caber na *fair usage quota* do Free Edition — a discussão "e com 10x/100x?" é feita nos ADRs, não no cluster.

### 5.1 ERP

| Dataset | Grão | Volume | Sujeiras intencionais |
|---|---|---|---|
| `erp_pedidos_cab` | 1 linha por pedido | ~800 mil | ~1,5% duplicatas exatas (reenvio de integração); ~0,5% quase-duplicatas (mesmo pedido com status atualizado); pedidos cancelados após faturamento |
| `erp_pedidos_itens` | 1 linha por item de pedido | ~3 mi | Itens com quantidade/valor negativos esporádicos; FK órfã (pedido referencia PDV ainda não cadastrado) |
| `erp_notas_fiscais` | 1 linha por NF | ~850 mil | NF de devolução misturada com venda; divergência centavos vs pedido |
| `erp_notas_fiscais_itens` | 1 linha por item de NF | ~3 mi | *Incluída em 2026-10-16 a pedido da engenharia (ADR-0003), para permitir faturamento por SKU/marca.* Valor do item diverge em centavos do item do pedido; itens de NF de devolução misturados com itens de venda; faturamento parcial (um item de pedido dividido em mais de uma NF) |
| `erp_clientes_pdv` | 1 linha por cliente (PDV, distribuidor ou rede) **por versão de cadastro**, com data de início de vigência | 40 mil clientes, ~70 mil versões | **PII**: CNPJ/CPF, nome do responsável, e-mail, telefone; ~2% CNPJs inválidos; mudanças de endereço, razão social e canal ao longo do tempo; grafias inconsistentes de cidade ("Sta Luzia", "Santa Luzia") |
| `erp_produtos` | 1 linha por produto (`product_id`, id interno nunca reaproveitado) por versão; o SKU é atributo | ~130 versões (92 produtos, ~80 ativos) | SKU descontinuado e **reaproveitado** para outro produto (novo `product_id`); EAN duplicado; descrição alterada |
| `erp_tabela_preco` | SKU × canal × vigência | ~8 mil | Vigências sobrepostas; reajuste retroativo. *Fora da fatia 1 (ADR-0006).* |
| `erp_hierarquia_comercial` | vendedor → supervisor → gerente → região, por vigência | ~500 | **Reorganização trimestral** (vendedor muda de supervisor/região), às vezes cadastrada dias depois do início da vigência — o primeiro SCD tipo 2 do projeto |
| `cep_municipio` (referência) | 1 linha por CEP | ~5 mil (os CEPs usados pelos clientes) | Sem sujeira: é a referência usada para padronizar a cidade do cadastro. *Incluída em 2026-10-19 (ADR-0006).* |

**Campos estruturais exigidos pela modelagem** (ADR-0006, 2026-10-19), além das colunas naturais de cada dataset:
- `erp_pedidos_cab` traz `sales_rep_id` (vendedor do pedido) e `customer_id`.
- `erp_notas_fiscais_itens` traz `order_id` e `order_item_number` (o item de pedido que está sendo faturado).
- `erp_clientes_pdv`, `erp_produtos` e `erp_hierarquia_comercial` trazem `valid_from` em cada versão; a extração full traz o histórico inteiro de versões.
- Os extratos chegam em Parquet em `/Volumes/core/landing/erp/<dataset>/<AAAA-MM-DD>/` (ADR-0004).
- **Carga inicial gerada em 2026-10-19:** colunas, regras, volumes reais e contagem de cada sujeira em [dicionario-de-dados-erp.md](dicionario-de-dados-erp.md).

### 5.2 Produção e estoque

| Dataset | Grão | Volume | Sujeiras |
|---|---|---|---|
| `wms_estoque_snapshot` | CD × SKU × dia | ~550 mil | Dias faltantes (CD não enviou); estoque negativo; um CD envia quantidade em caixas em vez de unidades durante 2 semanas |
| `mes_producao` | apontamento por ordem de produção | ~80 mil | Apontamentos com atraso de dias; mesma OP apontada duas vezes (retrabalho) |

### 5.3 Logística

| Dataset | Grão | Volume | Sujeiras |
|---|---|---|---|
| `tms_entregas` | 1 linha por entrega (pedido pode ter mais de uma) | ~900 mil | Timestamp de entrega anterior ao de saída; entregas sem pedido correspondente; fusos misturados |
| `tms_devolucoes` | 1 linha por devolução | ~25 mil | Motivo em texto livre; devolução sem entrega associada |

### 5.4 Sell-out (a fatia suja)

| Dataset | Grão | Volume | Sujeiras |
|---|---|---|---|
| `sellout_<fonte>` (14 fontes) | fonte × PDV × SKU do varejista × dia ou semana | ~2,5 mi no total | Layouts distintos por fonte (nomes de coluna, separador, encoding, datas em 3 formatos); atraso de D+2 a D+7; **reenvio do mês inteiro substituindo o anterior**; SKU do varejista ≠ SKU interno com de-para incompleto; linhas de subtotal no meio do arquivo; uma fonte adiciona a coluna `promo_flag` no meio da série (schema evolution) |

### 5.5 Externas

| Dataset | Grão | Volume | Sujeiras |
|---|---|---|---|
| `clima_diario` | cidade × dia (10 cidades, uma por CD/região) | ~5,5 mil | Gaps de medição |
| `feriados` | data × abrangência (nacional/UF/município) | ~1,2 mil | Feriados municipais incompletos |

### 5.6 Desbalanceamento regional (proposital, em todos os fatos)

SE 45% · S 20% · NE 18% · CO 10% · N 7% — para forçar conversas sobre skew em joins e agregações.

Nos dados gerados, essa é a distribuição dos PDVs. Em volume, o Sudeste chega a ~56%, porque as duas redes nacionais compram pela matriz em SP — o que também separa região geográfica de região comercial (ADR-0006).

## 6. Requisitos não funcionais

| Requisito | Definição no projeto |
|---|---|
| **SLA comercial** | Dados D-1 prontos até **8h BRT** (job diário em janela única de madrugada) |
| **SLA sell-out** | Dado disponível em até 2 dias após o **recebimento** do arquivo (não da data da venda) |
| **Custo** | Serverless com *fair usage quota*: tudo roda em **1 execução diária**; reprocessos são exceção justificada |
| **Retenção** | Bronze de datasets incrementais: tudo; bronze de datasets full: últimas 90 extrações; landing: 90 dias. Em produção, prazo legal de guarda fiscal ([ADR-0007](adr/0007-retencao-da-bronze-por-tipo-de-extracao.md), ajustado em 2026-10-19; antes: "bronze 90 dias") |
| **Reprocessamento** | Qualquer dia pode ser reprocessado sem gerar duplicata (idempotência é critério de pronto de toda fatia) |
| **LGPD** | PII só na bronze, com acesso restrito; não é copiada para silver/gold (minimização, [ADR-0004](adr/0004-ajustes-de-organizacao-nomes-landing-e-pii.md)); direito ao esquecimento = apagar na bronze e na landing + `VACUUM` |

## 7. Limitações do Databricks Free Edition que moldam o projeto

Confirmadas na documentação oficial (*Free Edition limitations*) e/ou observadas neste ambiente:

| Limitação | Impacto no projeto |
|---|---|
| Só compute **serverless**; sem configuração custom de cluster | Nada de dimensionar cluster; trade-offs de compute viram discussão de ADR |
| **1 SQL warehouse**, máximo 2X-Small | Dashboard e consultas ad-hoc disputam o mesmo warehouse |
| **Máx. 5 job tasks concorrentes** | DAGs enxutos; orquestração pensada em série |
| **1 pipeline Lakeflow ativo por tipo** | Pipelines `core` e por domínio rodam em série, encadeados por um Job (ADR-0002/0005) |
| ***Fair usage quota*** — estourar derruba o compute pelo resto do dia (em casos extremos, do mês) | Observado na prática (conta `aws` bloqueada). Volumes modestos, 1 execução/dia, agendamento pausado e validação sem processar dados antes de rodar (ADR-0005) |
| Sem R/Scala; storage só gerenciado; 1 workspace/metastore; sem APIs de conta | Sem external locations próprias. **Catálogos novos podem ser criados** em *default storage* via SQL (`CREATE CATALOG`), testado em 2026-10-16; via API/CLI (`databricks catalogs create`) falha com "Metastore storage root URL does not exist" |
| Acesso externo limitado a domínios confiáveis | APIs de clima/feriados podem ser inacessíveis do compute → plano B: gerar esses dados como fake também |
| Uso não comercial, sem SLA da plataforma | Ambiente de estudo; "em produção faríamos X" registrado em cada ADR |
