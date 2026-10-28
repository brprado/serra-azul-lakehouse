# ADR-0011: Montar os atributos tipo 1 da dim_customer pela última versão de cada cliente, e não pela versão aberta

- **Status:** aceito (2026-10-22, depois da revisão do `adr-reviewer` e da conferência pós-refresh) — emendado pelo [ADR-0012](0012-job-diario-extracao-simulada-idempotencia-e-verificacao.md) (D6: "prevenir ou só detectar" resolvido pelos dois, com uma linha por cliente e início antes do cálculo de versões e a verificação de unicidade no job; o tipo 1 passa a ter ordenação total e sai da mesma linha que o `is_latest`; o nome da `dim_sales_rep` segue a mesma regra)
- **Data:** 2026-10-22
- **Fatia:** fatia 1 — Comercial
- **Decisor:** Engenheiro de Dados Sênior (Claude) · **Acompanhamento:** usuário (pode vetar)
- **Relação:** emenda o [ADR-0006](0006-dimensoes-conformadas-identidade-scd-e-chaves.md) §2 ("os atributos de tipo 1 vêm da versão corrente"). Aplica à `dim_customer` a regra do `is_latest` do [ADR-0008](0008-pipeline-core-deduplicacao-qualidade-e-cadastros.md) §4 e o desempate da `dim_product` do [ADR-0010](0010-pipeline-commercial-fatos-chaves-e-membro-desconhecido.md) D12.

## Contexto

A `dim_customer` tem SCD por atributo (ADR-0006): cada atributo é tipo 1 **ou** tipo 2. Os rastreados (tipo 2) geram versão. Os de tipo 1 repetem o valor atual em todas as versões.

A tabela da Decisão do ADR-0006 listava só razão social e status como tipo 1. O código e a [modelagem](../modelagem-do-lakehouse.md) §2.4 usam seis: `legal_name`, `trade_name`, `person_type`, `is_document_valid`, `status` e `registered_date`.

O valor atual vinha da versão `is_current` da `silver.erp_customer`, como manda o ADR-0006 §2. Isso contradiz a regra do `is_latest` do ADR-0008 §4: "como é hoje" é a última versão, mesmo sem vigência aberta. É o mesmo defeito que o ADR-0010 D12 corrigiu na `dim_product`.

**Por que importa:**
- um *fan-out* da `dim_customer` inflaria "vendas por canal", a pergunta do Diretor Comercial;
- a contagem do fato igual à da silver é a base da conciliação da Controladoria (ADR-0010 D6).

O defeito foi achado ao escrever a modelagem, que o registrava como risco na §8.

## Decisão

**Os atributos tipo 1 da `dim_customer` vêm da última versão de cada cliente na silver, aberta ou não.**

O código está em [`src/pipelines/core/gold/dim_customer.sql`](../../src/pipelines/core/gold/dim_customer.sql), na CTE `current_values`:

```sql
SELECT customer_id, legal_name, trade_name, person_type, is_document_valid, status, registered_date
FROM silver.erp_customer
QUALIFY row_number() OVER (PARTITION BY customer_id ORDER BY valid_from DESC, valid_to DESC, _source_updated_at DESC) = 1
```

- **Desempate**, o mesmo da `dim_product` (ADR-0010 D12):
  1. o começo mais recente (`valid_from DESC`);
  2. no mesmo dia, a vigência que termina depois (`valid_to DESC`): a aberta (`9999-12-31`) vence;
  3. depois, a mais atualizada no ERP (`_source_updated_at DESC`).
- **O `JOIN` interno continua.** Ele foi escolha de implementação, e não do ADR-0006. É seguro porque:
  - as duas CTEs (`dated` e `current_values`) leem a mesma `silver.erp_customer`;
  - `customer_id` nunca é nulo: a silver para a carga se for (`customer_id_not_null`, `FAIL UPDATE`).
  - Portanto, todo `customer_id` com versão rastreada tem a sua última versão, e o `JOIN` não perde cliente.
- **Nada mais muda:** atributos rastreados, fusão de versões, `customer_key = xxhash64(customer_id, valid_from)`, `is_current`, `is_latest` e membro `-1`.

### Os dois defeitos que a mudança fecha

| Com `is_current` | O que aconteceria | Com a última versão |
|---|---|---|
| Cliente cuja última versão o ERP fechou (sem vigência aberta) | O cliente não tinha linha em `current_values`. O `JOIN` interno o tirava inteiro da dimensão, com todo o histórico. Todas as vendas dele iriam para o `-1` com `_is_customer_id_known = false`: o caso (a) do ADR-0010 D2, que diz "cadastro atrasado, FK nula ou LGPD". **Motivo falso** | O cliente fica, com todas as versões (ver "Como fica para quem consulta") |
| Cliente com duas versões abertas, **com inícios diferentes** | Duas linhas em `current_values`. O `JOIN` duplicava cada `customer_key` do cliente, e o `LEFT JOIN` dos fatos duplicava as linhas dele (*fan-out*). A contagem do fato deixaria de bater com a silver (ADR-0010 D6) | Uma linha por cliente em `current_values`. O `lead(valid_from)` corta a primeira versão, e só a que começa depois fica aberta |

### O que a mudança não fecha: duas versões com o mesmo início

- **O caso:** duas versões da silver com o mesmo `customer_id` e o mesmo `valid_from`, e atributos rastreados diferentes.
- **O que acontece na dimensão:**
  - elas abrem duas ilhas com a mesma `customer_key = xxhash64(customer_id, valid_from)`;
  - o `lead` dá a uma delas o intervalo vazio `[d, d)`;
  - qual fica aberta e qual é a `is_latest` não é determinístico, porque a ordem por `valid_from` empata.
- **Efeito nos fatos:**
  - a junção por vigência do pipeline não duplica: o intervalo vazio não casa com nenhuma data;
  - a junção `fact.customer_key = dim.customer_key`, o padrão "como era", **duplica** as linhas do fato.
- **Origem:** o defeito já existia, na chave do ADR-0006 §4. O caso também viola a chave da fonte, `cod_cliente` + `dt_inicio_vigencia` ([dicionário](../dicionario-de-dados-erp.md) §3.1).
- **Hoje:** 0 casos, contados pela regra de aviso `no_overlap_with_next_version` (ADR-0010 D11), que conta duas versões com o mesmo início como sobreposição.
- **Detecção automática:** a tarefa de verificação de unicidade da `_key` (ADR-0010 D13) vai conferir esse caso. Ela chega com o job `serra_azul_daily`, que ainda não existe.
- **Fica para o ADR do job:** prevenir (deduplicação determinística na dimensão) ou só detectar (D13).

### Divergência residual entre `is_latest` e o tipo 1

- O `is_latest` da dimensão é a maior `version_number`, numerada pela ordem de `valid_from`, sem desempate.
- O tipo 1 vem do desempate acima.
- Os dois só podem discordar no caso do mesmo início, descrito acima.

### Limitação: a ordenação do tipo 1 não é total

- Num empate de `valid_from`, `valid_to` e `_source_updated_at`, o `row_number` escolhe qualquer uma das linhas empatadas.
- Nesse caso, o tipo 1 pode variar entre recálculos.
- É a mesma limitação registrada no ADR-0010 D11: num empate total, a linha escolhida pode variar.

### Inativado não é vigência fechada

- `status = 'INATIVO'` é um atributo tipo 1. Vigência fechada é outra coisa: o ERP não mandou versão aberta para o cliente.
- **Medido em 2026-10-22:**
  - 1.234 clientes `INATIVO` hoje, todos com versão aberta: o ERP inativa com uma versão nova e não fecha a vigência;
  - 0 reativações;
  - 0 itens de venda com data igual ou posterior à inativação.
- **Consequência:** ninguém deve esperar clientes `INATIVO` no caso (b). O (b) do cliente vem de vigência total fechada, e hoje há 0 clientes assim.

### Como fica para quem consulta

Um cliente cuja última versão foi fechada:
- fica na dimensão, com `is_current = false` em todas as linhas e `is_latest = true` na última;
- os fatos com data dentro das vigências continuam com a `customer_key` da versão certa;
- os fatos com data depois do fim da última vigência caem no `-1` com `_is_customer_id_known = true`. É o caso (b) do ADR-0010 D2 ("evento fora da vigência total"): o **motivo correto**;
- na visão "como é hoje" (`customer_id` + `is_latest`), ele aparece com a razão social e o status da última versão, em vez de nulos.

### Validação

**Medição antes de qualquer refresh (2026-10-22, no warehouse):**

| Conferência | Valor |
|---|---|
| Clientes na `silver.erp_customer` | 39.773 |
| Clientes sem versão aberta | 0 |
| Clientes com duas versões abertas | 0 |
| Clientes com a última versão fechada | 0 |
| Clientes com algum atributo tipo 1 diferente entre "última versão" e "versão aberta" | 0 |
| `core.gold.dim_customer`: clientes / linhas | 39.773 / 47.167 (47.166 versões + o `-1`) |
| `customer_key` duplicadas | 0 |
| Linhas com `is_latest` / com `is_current` | 39.774 / 39.774 (incluem o `-1`) |

- **Clientes** = `customer_id` distintos, sem o `-1`. **Linhas** incluem o `-1`.

**Teste com casos literais (2026-10-22):**
- A lógica da dimensão rodou no warehouse sobre valores literais, no lugar da silver, nas duas formas: a antiga (commit `eba6d7f`) e a nova.
- O script ficou fora do repositório, no scratchpad da sessão do Sênior.

| Caso | Antiga | Nova |
|---|---|---|
| C1. Normal: 2 versões, a última aberta | 2 linhas, 2 chaves, tipo 1 = X2 | igual |
| C2. Última versão fechada (status `INATIVO`) | o cliente some da dimensão | 2 linhas, 2 chaves, tipo 1 = Y2; `is_current` em 0 linhas, `is_latest` em 1; a última vigência termina em 2026-01-01 |
| C3. Duas abertas, inícios diferentes | 4 linhas, 2 chaves (cada chave duplicada), tipo 1 misturado (Z1, Z2) | 2 linhas, 2 chaves, tipo 1 = Z2; a primeira é cortada pelo `lead` |
| C4. Mesmo início, atributos rastreados diferentes | 2 linhas, 1 chave | 2 linhas, 1 chave: **continua aberto**; intervalo vazio `[2025-01-01, 2025-01-01)` |
| C5. Mesmo início e fim, atributos iguais, atualização diferente | 2 linhas, 1 chave, tipo 1 misturado (K1, K2) | 1 linha, tipo 1 = K2 (desempate por `_source_updated_at`) |

- C2 e C3 mostram os dois defeitos fechados. C4 é o caso que fica para o ADR do job. C5 mostra o desempate.

**Pipeline:** `databricks bundle validate` OK; *validate-only* do `core` OK (update `3cfc0a`, 2026-10-22).

**Quando vale:**
- A `dim_customer` é materialized view. O precedente é o ADR-0009: a regra nova na `silver.erp_customer`, também uma materialized view, valeu para todo o histórico no refresh normal, sem *full refresh*. O refresh desta mudança vai confirmar.
- **Pressuposto:** num refresh seletivo da `dim_customer`, a silver não é recalculada, e as duas CTEs leem a mesma versão gravada dela.
- **Efeito esperado:** nenhuma linha muda. A correção fecha uma porta; não corrige dado de hoje.

## Resultados depois do refresh

O "antes" foi medido em 2026-10-22, antes de qualquer refresh. O "depois" foi medido no mesmo dia, depois do refresh seletivo da `dim_customer` (`databricks bundle run core --refresh core.gold.dim_customer`; os outros 27 fluxos do `core` ficaram `EXCLUDED`).

| Conferência | Antes | Depois |
|---|---|---|
| Update do `core` e duração do refresh | — | `9430ea`: ~70 s no total, dos quais ~11 s do fluxo `dim_customer` (planejamento ao fim) e o resto arranque do serverless |
| Linhas (com o `-1`) | 47.167 | 47.167 |
| Clientes | 39.773 | 39.773 |
| `customer_key` distintas | 47.167 | 47.167 |
| Linhas com `is_latest` | 39.774 | 39.774 |
| Linhas com `is_current` | 39.774 | 39.774 |
| Checksum | `12569720256` | `12569720256` |

- **Resultado:** a dimensão ficou idêntica, como esperado. A mudança vale para todo o histórico pelo refresh normal da materialized view, sem *full refresh*, como no precedente do ADR-0009.

- **Checksum** = `sum(xxhash64(<as 23 colunas da dimensão>) % 1000000007)`.

## Alternativas consideradas

| Alternativa | Por que não |
|---|---|
| Manter `is_current` com `JOIN` interno | Os dois defeitos: o cliente com a última versão fechada some da dimensão com todo o histórico e vai para o (a) com motivo falso; duas versões abertas duplicam a `customer_key` e as linhas dos fatos |
| `is_current` com `LEFT JOIN` | O cliente fica, mas com razão social, status e os outros tipo 1 nulos, justo quando a pergunta é "como é hoje". Duas versões abertas continuariam duplicando a chave |
| Expectation `FAIL UPDATE` quando um cliente tiver 0 ou 2+ versões abertas | Pararia a carga diária inteira, e o SLA das 8h do Diretor Comercial, por um problema de cadastro de um cliente. A silver já mede a sobreposição com `no_overlap_with_next_version` (ADR-0010 D11), e a tarefa de verificação do job vai conferir a unicidade da `_key` (ADR-0010 D13, a entregar) |
| Levar os tipo 1 para a struct rastreada e pegar o valor da linha `is_latest` da própria dimensão | Eles passariam a abrir versão nova: viram tipo 2. Isso muda a regra de SCD por atributo do ADR-0006, o que é quebra do **contrato da dimensão conformada** ([convenções](../convencoes.md)) |
| `max_by` por coluna, como no nome da `dim_sales_rep` | Precisa de um `max_by` por atributo (seis), sem desempate. Num empate, cada coluna pode vir de uma versão diferente: mistura de versões |
| Tipo 1 pela mesma ordenação do `is_latest` | O `is_latest` ordena só por `valid_from` e não é determinístico em empate. Alinhar exigiria mudar a ordenação da parte SCD 2, que é o caminho do caso do mesmo início, a decidir com o job |

## Consequências

- **Positivas:**
  - a `dim_customer` segue a regra do `is_latest`, como a `dim_product` (ADR-0010 D12), com o mesmo desempate;
  - um cliente encerrado não some: as vendas dentro da vigência mantêm a versão certa, e as de depois vão para o (b), com o motivo certo;
  - o tipo 1 deixa de ser um caminho de duplicação da `customer_key`. O caminho do mesmo início continua (ver acima);
  - custo baixo, por estimativa (não medido): a janela nova tem outro `ORDER BY` que as da dimensão, lê a silver (72.650 versões) uma segunda vez, e há o `JOIN`. A duração do refresh entra nos Resultados.
- **Negativas / dívidas assumidas:**
  - **mesmo início:** duas versões com o mesmo `valid_from` continuam duplicando a `customer_key` (hoje 0). Prevenir ou só detectar fica para o ADR do job;
  - a divergência residual entre `is_latest` e o tipo 1, no mesmo caso;
  - a ordenação do tipo 1 não é total: num empate dos três critérios, o tipo 1 pode variar entre recálculos;
  - duas versões abertas com inícios diferentes continuam sendo erro do ERP. A dimensão fica com a que começa depois, sem avisar; a métrica fica na silver (`no_overlap_with_next_version`);
  - **diferença com a `dim_sales_rep`:** ela tira o nome da última versão com `max_by(sales_rep_name, valid_from)`, a mesma regra sem o desempate por `valid_to` e `_source_updated_at`. Não muda agora; alinhar quando a `dim_sales_rep` for alterada;
  - a carga atual não exercita os defeitos (0 casos). A prova é o teste com casos literais, que ficou fora do repositório.
- **O que nos faria revisitar:**
  - `no_overlap_with_next_version` contar versões de cliente **com o mesmo início**: o caso deixa de ser teórico, e "prevenir ou detectar" passa a ser urgente;
  - `no_overlap_with_next_version` contar versões abertas com inícios diferentes: a dimensão está escolhendo em silêncio, e o caso vai para o time do ERP;
  - o (b) do cliente subir nos fatos (`customer_version_valid_on_order_date` / `..._on_issue_date`): clientes encerrados com venda depois do fim da vigência;
  - o negócio pedir histórico de razão social ou status: eles virariam tipo 2, por um ADR novo;
  - a conferência depois do refresh mostrar alguma linha diferente;
  - uma mudança na `dim_sales_rep`: alinhar o desempate do nome.

## Perguntas difíceis

- **"Se hoje são 0 casos, por que mexer?"** Porque o defeito apareceria em silêncio: um `-1` com motivo falso ou linhas duplicadas nos fatos. O teste com casos literais mostra os dois (C2 e C3). A correção custa uma janela e alinha a `dim_customer` à `dim_product`.
- **"E se o ERP encerrar um cliente (fechar a última versão)?"** Antes, o cliente sumia da dimensão, e todas as vendas dele, inclusive as antigas, iam para o (a). Agora ele fica, e só as vendas depois do encerramento vão para o (b). Inativar não é encerrar: o ERP inativa com uma versão nova e aberta.
- **"E se o ERP mandar duas versões abertas do mesmo cliente?"**
  - Com inícios diferentes: o `lead(valid_from)` corta a primeira, só a que começa depois fica aberta, e o tipo 1 vem dela (C3).
  - Com o mesmo início: a `customer_key` duplica (C4); ver "O que a mudança não fecha".
  - Nos dois casos, `no_overlap_with_next_version` avisa na silver, e a tarefa de verificação do job vai conferir a unicidade quando existir (D13). A correção é na fonte.
- **"E se a extração de clientes chegar 2 dias atrasada?"**
  - A silver é recalculada da última extração que chegou (ADR-0008 §3). Até a nova chegar, vale o cadastro anterior.
  - O atraso é **silencioso**: detectar fonte que não chegou (*freshness*) é tema da fatia 2.
  - Um cliente novo fica no (a) e se corrige na primeira execução depois da chegada do extrato (ADR-0006 §6).
  - Um encerramento retroativo também só aparece então, e muda dias já reportados: as vendas posteriores ao fim da vigência passam para o (b). É o trade-off aceito no ADR-0006 (um mês já reportado pode mudar).
  - Esta decisão não muda esse comportamento.
- **"Por que não parar a carga quando o cadastro vier errado?"** Ver Alternativas: um cliente com cadastro errado não justifica deixar o Diretor Comercial sem o D-1.

## Em produção faríamos diferente?

A regra se sustenta em produção. O que muda fica em volta dela:
- **Contrato de dados da fonte com o time do ERP:** cada cliente tem no máximo uma versão aberta, e cliente ativo tem exatamente uma.
  - **Cliente ativo** = `status = 'ATIVO'`, um atributo tipo 1.
  - Vigência aberta é outro conceito. Um cliente sem vigência aberta (encerrado) é legítimo.
- **Violação tratada por alerta e chamado para a origem, sem parar a carga.** Duas versões abertas seriam violação do contrato de dados da fonte. `FAIL UPDATE` continua rejeitado pelo SLA das 8h, e a dimensão segue correta pela última versão.
- **Verificação de unicidade da `_key`** no job (ADR-0010 D13, a entregar) e no CI, antes do deploy.

A decisão não foi moldada por limitação do Free Edition.

**Em 10x** (~400 mil clientes, ~730 mil versões; estimativa, não medição): a janela nova, a segunda leitura da silver e o `JOIN` somam uma ordenação e um *shuffle* de menos de 1 mi de linhas. Continua pequeno.

**Em 100x** (~4 mi de clientes, ~7 mi de versões):
- o que pesa é recalcular a dimensão inteira a cada execução, dívida já registrada (ADR-0008; [modelagem](../modelagem-do-lakehouse.md) §8);
- num cálculo incremental, aparece o custo clássico do tipo 1 numa SCD 2: uma mudança de tipo 1 na última versão reescreve o valor em **todas** as versões do cliente;
- a regra da última versão vale igual.

## Conceitos para estudar

- SCD por atributo: na mesma dimensão, cada atributo é tipo 1 ou tipo 2;
- tipo 6 × tipo 7, e por que o projeto usa o 7:
  - no tipo 6, o **mesmo** atributo é guardado duas vezes: numa coluna histórica (tipo 2) e numa coluna "atual" (tipo 1), reescrita em todas as linhas da chave durável;
  - o projeto não usa o tipo 6. O "como é hoje" dos atributos rastreados vem do tipo 7: o `_id` durável no fato, juntado com a linha `is_latest` da dimensão (ADR-0006 §5, ADR-0008 §4, ADR-0010; [modelagem](../modelagem-do-lakehouse.md) §2.7 e §2.10);
- `is_current` × `is_latest`: vigência aberta × última versão;
- status × vigência: inativar não é encerrar;
- `QUALIFY` + `row_number()` com desempate determinístico, e o que acontece quando a ordenação não é total;
- chave substituta derivada de `(id, data)`: por que duas versões com o mesmo início colidem;
- *fan-out*: por que uma junção com dimensão de chave não única multiplica as linhas do fato;
- membro desconhecido e o motivo do `-1`: casos (a) e (b).
