# ADR-0013: Publicar o dashboard comercial pelo bundle, com a última data carregada como referência e atualização no fim do job diário

- **Status:** aceito (2026-11-01)
- **Data:** 2026-11-01
- **Fatia:** fatia 1 — Comercial
- **Decisor:** Engenheiro de Dados Sênior (Claude) · **Acompanhamento:** usuário (pode vetar) · **Veto:** nenhum.
  - Em 2026-10-28, o usuário aprovou o plano e foi avisado da mudança do D10: o achado do cache de 24 h e os três ajustes (a tarefa no job depois da verificação, `embed_credentials: true` e o frescor por propriedade do dado).
  - Depois aprovou o deploy e a execução do job, em 2026-11-01.
  - **Ajustes da revisão do `adr-reviewer`** (2026-11-01): o filtro de região geográfica (D13), os nomes do dashboard (D14), a retentativa do D10 e a correção do simulador (seção "Achado durante a revisão"). Foram decididos pela sessão principal e implantados em 2026-11-01, às 13:45 UTC, depois da confirmação visual do usuário (Resultados). O usuário foi informado desses ajustes em 2026-11-01, antes do deploy, e não vetou.
- **Relação:** entrega o dashboard previsto no [ADR-0005](0005-orquestracao-por-job-e-codigo-em-bundle.md) §2. Aplica a região comercial e o filtro geográfico do [ADR-0006](0006-dimensoes-conformadas-identidade-scd-e-chaves.md) §7 e o contrato dos fatos do [ADR-0010](0010-pipeline-commercial-fatos-chaves-e-membro-desconhecido.md). As linhas de status abaixo passam a valer de fato quando este ADR for aceito:
  - [ADR-0012](0012-job-diario-extracao-simulada-idempotencia-e-verificacao.md):
    - **emenda** do D7 e do D8: a `verify_uniqueness` deixa de ser a última tarefa, e o job ganha a 5ª tarefa, `refresh_dashboard`, depois dela (D10);
    - **pendência fechada:** as 3 contagens novas do D7 "ainda não rodaram no job". Rodaram em 2026-11-01: 10 zeros;
    - **defeito corrigido:** o simulador entregava cancelamentos de D + 1 na extração de D (ver "Achado durante a revisão"). É correção de implementação, que restabelece o contrato do ADR-0012, e não decisão nova;
  - [ADR-0005](0005-orquestracao-por-job-e-codigo-em-bundle.md) §3, **emenda:** a sequência do job ganha a atualização do dashboard (D10);
  - [ADR-0003](0003-fatos-de-vendas-e-faturamento.md), **dívida resolvida:** "o dashboard precisa deixar claro qual número é qual" (D1). Segue o precedente do ADR-0012, que registrou na linha de status do ADR-0010 o fechamento de uma dívida.
- **Sem emenda ao [ADR-0010](0010-pipeline-commercial-fatos-chaves-e-membro-desconhecido.md) D13:** a decisão de verificar a unicidade numa tarefa do job continua de pé; só a posição da tarefa mudou. **Ressalva:** o "tarefa final" do D13 e a linha de status do ADR-0005 que o cita ficam desatualizados. O caminho de leitura é 0010 → 0012 → 0013.
- **Exceção registrada:** à convenção "nome de catálogo nunca no SQL" (D9). A regra do [ADR-0002](0002-motor-de-transformacao.md) vale para o SQL dos pipelines e não muda. Por isso, o ADR-0002 não é emendado.

## Contexto

Os pipelines e o job publicam os fatos com D-1 (ADR-0010, ADR-0012). Faltava o entregável visível da fatia 1: um painel que responda ao Diretor Comercial "quanto vendi ontem, por região, marca e canal?", com o SLA de D-1 até 8h. Duas definições de "venda" convivem (ADR-0003), e o painel precisa dizer qual número é qual. O Free Edition tem um único SQL warehouse, 2X-Small, e o dashboard publicado serve resultados em cache por até 24 h. O [caso de negócio](../caso-de-negocio.md) §3 também pergunta "como estou contra a meta?", mas não existe fonte de metas (§4 e §5).

## Decisão

**Um dashboard AI/BI de duas páginas, descrito no bundle, lê os fatos e as dimensões direto, usa a última data carregada como referência e é atualizado por uma tarefa no fim do job diário.**

| # | Decisão |
|---|---|
| D1 | "Quanto vendi" = `fact_sales` sem cancelados; o faturamento fica numa segunda página, para o mesmo diretor, e os números não se somam |
| D2 | Data de referência = a última data carregada, e não `current_date - 1`; o usuário pode escolher outra |
| D3 | Comparação com o mesmo dia da semana anterior (referência − 7), com o viés de maturação dos cancelamentos medido e avisado |
| D4 | Frescor só com propriedades do dado |
| D5 | Meta fora da fatia: lacuna registrada |
| D6 | Sem tabela agregada: as consultas leem fatos e dimensões |
| D7 | Datasets já agregados no SQL, com uma exceção para a contagem de pedidos |
| D8 | Membro `-1` visível: `LEFT JOIN` e rótulos DESCONHECIDA/DESCONHECIDO |
| D9 | Exceção à convenção de catálogo: `commercial.gold` escrito no JSON |
| D10 | Atualização pela tarefa `refresh_dashboard`, no fim do job, com 1 retentativa |
| D11 | `embed_credentials: true` |
| D12 | Operação: warehouse pelo nome, edição só pelo JSON, JSON fora do `sync` |
| D13 | Filtro "Região geográfica (cliente)" nas duas páginas |
| D14 | Nomes dentro do dashboard em português |

Código: [`resources/commercial_sales.dashboard.yml`](../../resources/commercial_sales.dashboard.yml), [`src/dashboards/commercial_sales.lvdash.json`](../../src/dashboards/commercial_sales.lvdash.json) (as consultas estão nos campos `queryLines` dos datasets), [`databricks.yml`](../../databricks.yml) (variáveis `warehouse_id` e `catalog_commercial`, exclusão no `sync`), [`resources/serra_azul_daily.job.yml`](../../resources/serra_azul_daily.job.yml) (tarefa `refresh_dashboard`) e [`data_gen/erp/erp_initial_load.py`](../../data_gen/erp/erp_initial_load.py) (função `order_status_as_of`, corrigida).

**O que o painel mostra.** Cada página tem cinco filtros: data de referência, região comercial, região geográfica (cliente), marca e canal.

| Página | Fato | Indicadores | Gráficos |
|---|---|---|---|
| Vendas (pedidos) | `fact_sales`, sem cancelados | Venda líquida (R$), volume (hL) e pedidos, cada um com a variação absoluta e em % contra a referência − 7 | Venda por região comercial, por marca e por canal; série diária de 8 semanas; tabela região × marca × canal × região do cliente |
| Faturamento (NFs) | `fact_invoice`, com o sinal das devoluções | Faturado líquido (R$) e devoluções (R$), com as variações | Faturado por região comercial, por marca e por canal; série diária de 8 semanas |

São 9 datasets: para cada página, as datas disponíveis, o frescor, os indicadores e a série; a tabela de combinações só existe nas vendas.

### D1. "Quanto vendi" é o pedido; o faturamento fica numa página separada, para o mesmo diretor

- **Vendas (página 1):** `fact_sales`, sem os cancelados (`NOT is_cancelled`, ADR-0003). Medidas: valor líquido (`net_amount`), volume em hL e pedidos distintos.
- **Faturamento (página 2):** `fact_invoice`, pela data de emissão. A soma de `net_amount` já desconta as devoluções, que vêm negativas do fato (ADR-0010 D5).
- **Os números não se somam.** O texto de cada página diz o que é o número e avisa que pedido e NF são eventos diferentes, em datas diferentes.
- **Por que a venda é o pedido:** a pergunta do diretor é a demanda capturada ontem. O faturamento atrasa o pedido de 0 a 2 dias.
  - Dos pedidos de 2026-10-26 (segunda), ~38% foram faturados no próprio dia, ~42% no dia seguinte e ~15% dois dias depois.
  - Os itens de NF de venda emitidos em 2026-10-26 vêm de 3.527 itens de pedidos do próprio dia, 427 de sábado, 85 de domingo e ~190 de dias anteriores.
  - Na segunda, a parte atrasada vem do fim de semana, que vende pouco. Por isso o faturado líquido de segunda (R$ 4.438.234,58) fica muito abaixo da venda (R$ 11.976.134,57).
- **Para quem é a página 2:** para o próprio diretor comercial. A diferença entre vendido e faturado, ou seja, os pedidos ainda não faturados, é a pergunta seguinte a "quanto vendi".
  - **Não é o painel da Controladoria:** não tem preço médio, desconto nem conciliação fiscal. Esse seria outro entregável, quando a Controladoria for o stakeholder.
- **Por que agora:** o ADR-0003 manteve dois fatos na fatia 1 e mandou o painel dizer qual número é qual. O custo marginal é baixo: 4 datasets de 1 a 3 s.
- **Fecha a dívida do ADR-0003:** cada número tem a sua página, o seu rótulo e a sua definição.

### D2. Data de referência = a última data carregada

- **Padrão:** a maior `order_date` do `fact_sales` (página 1) e a maior `issue_date` do `fact_invoice` (página 2). Não é `current_date - 1`.
- **Por quê:** "ontem" pelo relógio depende de o job ter rodado. Com o job atrasado ou falho, o painel mostraria um dia vazio ou incompleto. Com a última data carregada, o painel mostra o último dia que existe, e o frescor (D4) diz qual é.
- **O usuário pode escolher outra data** no filtro "Data de referência".
- **Mecanismo:**
  - parâmetro `data_ref`, do tipo `STRING`, em cada dataset que depende da data;
  - a opção padrão é a sentinela "Última data carregada";
  - cada consulta calcula `coalesce(try_to_date(:data_ref, 'yyyy-MM-dd'), max(order_date))`. A sentinela não é data, o `try_to_date` dá nulo, e vale o `max`;
  - o filtro é de seleção única, alimentado por um dataset com a sentinela e cada data carregada, em texto `AAAA-MM-DD`.
- **Cada página tem a sua referência.** As duas são independentes, e o padrão de uma pode ser diferente do da outra se `max(order_date)` e `max(issue_date)` divergirem. No deploy, as duas eram 2026-10-26; depois da execução do job de 2026-11-01, as duas são 2026-10-31.

### D3. Comparação com o mesmo dia da semana anterior

- Cada indicador é comparado com a referência − 7.
- **Por quê:** a venda de bebida varia muito com o dia da semana. Segunda contra domingo diria pouco.
- **Custo 1, feriado:** um feriado na semana anterior distorce a comparação. Exemplo: 2026-10-19 contra 2026-10-12, feriado nacional. O texto da série avisa ("fins de semana e feriados vendem pouco"). A `dim_date` ainda não tem feriados: eles entram na fatia 5 ([modelagem](../modelagem-do-lakehouse.md) §4.1).
- **Custo 2, maturação dos cancelamentos:** a venda exclui os pedidos cancelados até a carga, e uma parte dos pedidos de um dia é cancelada nos dias seguintes. A referência e a comparação têm idades diferentes.
  - **Medido** nos pedidos de 2026-10-01 a 2026-10-24, em % do total do dia:

| Cancelado em | Pedidos | Valor | Volume |
|---|---|---|---|
| D + 0 | 0,89% | 0,94% | 0,98% |
| D + 1 | 1,08% | 1,20% | 1,24% |
| D + 2 a D + 7 | 0,19% | 0,25% | 0,27% |
| D + 8 ou depois | 0,08% | 0,09% | 0,06% |

  - Visto em D + 1, o dia de referência ainda vai perder 1,35% dos pedidos, 1,54% do valor e 1,57% do volume. O dia de comparação, uma semana mais velho, já perdeu quase tudo.
  - **Viés na variação:** ~1,3 p.p. a mais em pedidos e ~1,5 p.p. a mais em valor e volume.
  - **O painel avisa**, no texto da página de vendas.
  - **Até a extração de 2026-11-01, o viés real foi menor:** o vazamento do simulador (ver "Achado durante a revisão") já tirava da foto de D parte dos cancelamentos de D + 1.

### D4. Frescor só com propriedades do dado

- **"Dados até"** = a maior data do evento (`order_date` ou `issue_date`).
- **"Carregados em"** = `max(_updated_at)`, convertido para `America/Sao_Paulo`.
- **Nada calculado com `current_date` ou `current_timestamp`.**
- **Por quê:** o dashboard publicado guarda o resultado em cache por até 24 h, e "*Using `current_timestamp()` or similar functions in your SQL query does not invalidate the dashboard-level cache*" ([caching](https://docs.databricks.com/aws/en/dashboards/caching)). Um "atraso em dias" seria o da hora da consulta, e não o da hora em que o diretor olha. Poderia dizer "em dia" com dado velho.
- **Frescor e indicadores vêm de datasets diferentes.** Cada um tem a sua entrada de cache: a página de caching fala de datasets, e a granularidade exata não é documentada. Sob um cache *best-effort*, os dois podem vir de gerações diferentes. É raro.
- **Custo:**
  - "Carregados em" **não muda numa recarga** sem dado novo, porque o `_updated_at` é o instante da ingestão (ADR-0010 D7), e não o da execução;
  - "Carregados em" **avança num reenvio** sem dado novo: o `AUTO CDC` renova o `_ingested_at` das linhas reenviadas (ADR-0012, Resultados). O painel diria "carregados agora" sem nenhum dado novo.

### D5. Meta fora da fatia: lacuna registrada

- O caso de negócio (§3) cita "como estou contra a meta?". Não há fonte de metas (§4 e §5).
- O painel avisa: "a comparação com a meta ainda não tem fonte de dados".
- **Quando a fonte existir, o grão dela decide a junção.** Provavelmente região comercial ou vendedor × mês, porque "mudar de região comercial muda a meta do vendedor" ([modelagem](../modelagem-do-lakehouse.md) §2.4; ADR-0006). **Não decidido agora.**
- **A lacuna não tem dona** em nenhuma fatia do roadmap. Fica registrada no post-mortem, em "o que ficou de fora".

### D6. Sem tabela agregada

- As consultas leem os fatos e as dimensões, com junção por igualdade de `_key` (ADR-0006 §5). A junção por vigência já foi feita no pipeline.
- **Medido no warehouse 2X-Small:**

| Situação | Tempo por dataset |
|---|---|
| A frio | 0,8 a 2,2 s |
| Com cache | 0,3 a 0,6 s |
| Warehouse parado (partida) | ~19,8 s |

- **Gatilho para criar um agregado:** um dataset passar de ~10 s.

### D7. Datasets já agregados no SQL, com uma exceção

- **Regra:** cada dataset chega agregado ao widget, com dezenas a ~3 mil linhas (contagens na tabela do D13). Até 100 mil linhas e 100 MB, o filtro roda no navegador ([limites](https://docs.databricks.com/aws/en/dashboards/limits)).
- **Exceção: os indicadores de vendas guardam o pedido no grão** (dia × região comercial × região geográfica × marca × canal × pedido). São 11.211 linhas em 2026-10-26 e 3.341 em 2026-10-31, sempre com 2 dias.
  - "Pedidos" é contagem distinta, e não é aditiva: um pedido com duas marcas conta uma vez em cada marca.
  - Somando por marca, 2026-10-26 teria 5.705 pedidos, contra 2.454 reais.
  - Com o pedido no grão, o filtro de região, marca ou canal recalcula a contagem distinta certa.
- **As variações (absoluta e %) são medidas do dataset**, e não colunas da consulta. São recalculadas sob os filtros. Exemplo: `(SUM(net_amount_ref) - SUM(net_amount_prev)) / NULLIF(SUM(net_amount_prev), 0)`.
- **Na tabela de combinações**, "pedidos" é contagem distinta dentro da combinação. A descrição do widget avisa que a coluna não se soma.

### D8. Membro `-1` visível

- **`LEFT JOIN` nas dimensões, com `coalesce`** para os rótulos DESCONHECIDA (região comercial, região geográfica, marca) e DESCONHECIDO (canal). São os mesmos rótulos da linha `-1` de cada dimensão.
- **O texto do painel** diz: DESCONHECIDA ou DESCONHECIDO = cadastro ausente ou fora da vigência na data do documento (os casos (a) e (b) do ADR-0010 D2); fica no total.
- **Regra:** o total do painel tem de bater com o fato.
- **Na data padrão do deploy (2026-10-26):** R$ 1.823,12 em canal DESCONHECIDO, nos dois fatos. 0 em região e em marca.
  - São 3 pedidos de 3 clientes cujo cadastro chegou depois da extração de 2026-10-27: C001683 (R$ 1.060,80), C032640 (R$ 187,22) e C037315 (R$ 575,10).
  - Os 3 pedidos são de 2026-10-26 e foram faturados no mesmo dia, sem divergência de centavos. Por isso o valor é igual nos dois fatos.
- **Depois da execução de 2026-11-01:** cliente `-1` = 0 nos dois fatos. Os clientes fora da carga chegaram, e a recomputação corrigiu as linhas (ADR-0006 §6).
- **Custo:** uma `_key` do fato sem linha na dimensão cairia no mesmo balde do `-1`, sem aviso. O total bate, mas o rótulo esconde a causa. A `verify_uniqueness` não confere essa integridade (fato → dimensão): ela confere a unicidade da `_key` e a contagem do fato contra a silver (ADR-0012 D7).
- **Quando isso pode acontecer:** a `_key` do fato nasce da dimensão, no cálculo do `commercial`. Uma `_key` sem linha só aparece se o `core` republicar as dimensões com versões diferentes e o `commercial` não recalcular:
  - o `commercial` falhar depois do `core`. Nesse caso, a verificação e a atualização do dashboard nem rodam;
  - ou o `core` rodar sozinho, fora do job.
  - O `commercial` rodar sem o `core` é inofensivo: as chaves continuam as da última dimensão publicada.
- **Sem uma 11ª contagem na verificação, por enquanto (YAGNI):** depois do go-live, o simulador não muda versões de cadastro (ADR-0012 D1). Nenhuma `_key` some. O `done-checker` mede a FK órfã no fechamento da fatia.
  - **Gatilhos para criar a contagem:** o `done-checker` achar algum caso; o `core` passar a rodar fora do job; uma fonte real, com versões que mudam.

### D9. Exceção à convenção: o catálogo do domínio escrito no JSON

- **A convenção:** nome de catálogo nunca é escrito no SQL; vem de `${catalog_core}` / `${catalog_<domínio>}` ([convenções](../convencoes.md)).
- **O que o bundle oferece:** um catálogo padrão e um schema padrão por dashboard (`dataset_catalog` e `dataset_schema`). Um nome qualificado no SQL vence o padrão ([recursos do bundle](https://docs.databricks.com/aws/en/dev-tools/bundles/resources), [API de dashboards](https://docs.databricks.com/api/lakeview/v1/dashboard)).
- **O `.lvdash.json` não interpola variáveis do bundle.** A documentação não fala disso. O `data-engineer` testou numa cópia, com `databricks bundle validate -o json`: `${var.x}` dentro do JSON não é interpolado.
- **Escolha:**
  - `dataset_catalog: ${var.catalog_core}` e `dataset_schema: gold`: as dimensões ficam sem prefixo (`dim_sales_rep`);
  - `commercial.gold.fact_*` escrito por extenso, uma vez por consulta (num CTE no topo; nas duas listas de datas, direto no `FROM`), com um comentário que explica a exceção.
- **Por que aceitar:**
  - o bundle só tem um catálogo padrão por dashboard, e as consultas leem dois;
  - o JSON não interpola variáveis;
  - o projeto tem um único target (ADR-0005). O risco que a convenção evita, um ambiente ler o catálogo de outro, não existe aqui.
- **Por que não as views de consumo agora** (o caminho de produção, abaixo):
  - publicar uma view por pipeline SDP tem documentação contraditória. A [referência de `CREATE VIEW`](https://docs.databricks.com/aws/en/ldp/developer/ldp-sql-ref-create-view) diz que existe `CREATE VIEW` em pipeline com Unity Catalog, com publicação padrão. A [página de conceitos](https://docs.databricks.com/aws/en/ldp/concepts/) diz que as views só podem ser usadas dentro do mesmo pipeline. Não foi confirmado no Free Edition, e testar exige uma execução;
  - uma materialized view larga materializaria ~3 mi de linhas a cada execução;
  - uma view criada fora do pipeline, num SQL de setup, voltaria a escrever o nome do catálogo.
- **Modo de falha (custo):** se `catalog_commercial` mudar, o deploy passa, e os pipelines gravam no catálogo novo. O painel continua lendo `commercial`, **em silêncio**. Com um segundo target, o painel misturaria as dimensões do target (interpoladas) com os fatos do literal.
  - **Guarda observável:** um comentário junto da variável `catalog_commercial` no `databricks.yml` avisa que mudar o valor exige mudar o JSON.
- **Custo:** se `catalog_commercial` mudar, as 9 consultas do JSON mudam junto.

### D10. Atualização pela tarefa `refresh_dashboard`, no fim do job

| Tarefa | Tipo | Depende de | Retentativas | Timeout | Tempo medido |
|---|---|---|---|---|---|
| `refresh_dashboard` | `dashboard_task` | `verify_uniqueness` | 1, com 60 s de intervalo | 600 s | 42 s (2026-11-01, 1ª tentativa, ainda com 0 retentativas configuradas) |

- **Sequência do job:** `simulate_erp_extraction` → `core` → `commercial` → `verify_uniqueness` → `refresh_dashboard`.
- **Warehouse:** a tarefa não define `warehouse_id` e usa o do dashboard ([tipos de tarefa no bundle](https://docs.databricks.com/aws/en/dev-tools/bundles/job-task-types)).
- **Sem `subscription`:** a entrega de e-mail não está confirmada no Free Edition. O teste fica para a fatia 2, junto com o da notificação de falha do job: é a mesma dúvida, com o mesmo teste barato.
- **Mudança em relação ao plano inicial.** O plano previa "sem atualização agendada; consulta ao abrir". O achado do cache mudou a decisão:
  - o dashboard publicado guarda o resultado por até 24 h, *best-effort*, e pode servir resultado velho mesmo com o dado mudado;
  - "*a schedule is the only reliable way to refresh the dashboard cache*" ([caching](https://docs.databricks.com/aws/en/dashboards/caching));
  - sem atualização, às 8h o diretor poderia ver o dia anterior ao D-1, guardado da visualização da véspera;
  - o usuário foi avisado da mudança em 2026-10-28, antes de aprovar o deploy, e não vetou.
- **Por que depois da verificação.** Primeiro, o que a ordem **não** compra:
  - com o job diário às 05:00, a entrada de cache do dia anterior já tem ~24 h quando a execução de hoje falharia. Às 8h, a visualização consulta o warehouse e mostra o dado novo, possivelmente duplicado;
  - a "proteção" do cache só existe em execuções manuais irregulares;
  - as 24 h contam a partir de quando o resultado foi obtido. É uma leitura provável, e não documentada: a página diz "*if the results were retrieved less than 24 hours ago*".
  - **Os motivos que sobram:** não gastar o warehouse para publicar no painel um dia que a verificação acusou, e a simplicidade de uma sequência em série.
  - **A verificação continua sendo alarme, e não portão** (ADR-0012 D7). O caminho de produção continua sendo o *write-audit-publish* (ADR-0012).
  - **Alarme visível ao consumidor (fatia 2):** a verificação grava o resultado numa tabela, e o dashboard mostra "verificação ok" ou "falhou em DD/MM", junto com a notificação de falha.
- **Por que 1 retentativa:** o mesmo motivo da verificação (ADR-0012 D8). Ela separa a falha passageira (partida a frio do warehouse) da falha real, que se repete. A atualização é idempotente: rodar de novo só refaz o cache. Se as duas tentativas falharem, nenhum dado se perde, só o frescor atrasa: o job fica vermelho, e o cache expira em até 24 h.
- **Emenda ao ADR-0012:** a `verify_uniqueness` deixa de ser a última tarefa (D7), e a tabela do D8 ganha uma 5ª linha.

### D11. `embed_credentials: true`

- **Com `true`:** as consultas rodam com a credencial de quem publicou, e o cache é compartilhado. A atualização feita pelo job serve a todos.
- **Com `false`:** cada visualizador tem o próprio cache. Os visualizadores "*can't view refreshed results generated by the Run as identity*" ([tarefa de dashboard](https://docs.databricks.com/aws/en/jobs/tasks/dashboard)).
- **A premissa, corrigida na revisão:** o job não tem `run_as`. Ele roda como quem implantou, que é o único usuário e o único visualizador. Neste workspace, com `false`, a atualização chegaria a ele.
- **Por que `true`:** o painel modela outro cenário, em que o diretor não é quem publica. Com `false`, a atualização feita pelo "Run as" não chegaria a ele.
- **Risco aceito:** com credencial embutida, o dashboard "*might expose data to users who have not been granted direct access*" ([compartilhar dashboards](https://docs.databricks.com/aws/en/dashboards/share/share)). A credencial lê tudo o que o publicador lê, **inclusive a `core.bronze`, com PII**.
  - **O que protege é o conteúdo publicado ser fixo:** o JSON é revisado no Git; o visualizador não escreve SQL; o parâmetro é ligado à consulta, e não concatenado nela.
- **Em produção:** `embed_credentials: true` com um *service principal* de menor privilégio (`SELECT` só em `commercial.gold` e `core.gold`), e os visualizadores num grupo com `CAN VIEW`. Aí a credencial não alcança a PII nem se o JSON mudar.

### D12. Operação

- **Warehouse pelo nome:** a variável `warehouse_id` vem de um `lookup` do warehouse `Serverless Starter Warehouse`, e não do id. O id muda se o warehouse for recriado; o nome é o que se lê na interface.
- **O dashboard se edita só pelo JSON no Git.** Uma edição pela interface deixa o workspace diferente do arquivo. O próximo deploy falha até alguém escolher um lado ([recursos do bundle](https://docs.databricks.com/aws/en/dev-tools/bundles/resources), [comandos do bundle](https://docs.databricks.com/aws/en/dev-tools/cli/bundle-commands)):
  - `databricks bundle generate dashboard` traz a edição para o JSON;
  - `databricks bundle deploy --force` a descarta.
- **`src/dashboards/*.lvdash.json` fica fora do `sync`.** Sincronizado como arquivo, o JSON viraria uma segunda cópia do dashboard no workspace. O dashboard vai pelo recurso `dashboards` do bundle.
- **Sem `parent_path`:** o comportamento não é documentado. Confirmado na prática pelo deploy: o dashboard ficou em `.../.bundle/serra_azul/prod/resources/`, com os outros recursos.

### D13. Filtro "Região geográfica (cliente)" nas duas páginas

- **O problema:** o ADR-0006 §7 promete a região geográfica do cliente como filtro secundário. A expansão no Nordeste é uma pergunta de mercado. A primeira versão do painel não tinha o filtro (achado bloqueante da revisão).
- **Escolha:**
  - o filtro usa `dim_customer.geo_region`, pela `customer_key` do fato. É a versão do cliente na data do documento: `order_date` nas vendas e `issue_date` no faturamento (ADR-0010 D1b);
  - o `-1` aparece como DESCONHECIDA;
  - a coluna entra em 5 datasets (`vendas_kpi`, `vendas_combinacao`, `vendas_serie`, `faturamento_kpi` e `faturamento_serie`) e na tabela de combinações, onde faz parte do grão.
- **As linhas não cresceram.** Há uma dependência funcional: pedido → cliente → região. Medido antes e depois, iguais:

| Dataset | Linhas (2026-10-31 / 2026-10-26) |
|---|---|
| `vendas_kpi` | 3.341 / 11.211 |
| `vendas_combinacao` | 49 / 55 |
| `vendas_serie` | 2.710 / 2.707 |
| `faturamento_kpi` | 104 / 100 |
| `faturamento_serie` | 2.830 / 2.828 |

- **Teste em 2026-10-31:**
  - **NORDESTE:** R$ 525.729,05 e 143 pedidos, contra R$ 487.415,90 na referência − 7 (+7,86%);
  - **SUDESTE:** R$ 1.949.529,42 e 321 pedidos.
- **Custo:** nos dados sintéticos, o filtro é quase redundante com a região comercial. O gerador atribui o vendedor pela região do cliente. As duas só diferem em CONTAS NACIONAIS, que geograficamente fica no SUDESTE (~25% do valor).
- **Sem emenda ao ADR-0006:** o filtro cumpre o §7.

### D14. Nomes dentro do dashboard em português

- **Regra:** nomes de datasets, o parâmetro e as colunas de apresentação (textos de frescor) ficam em português; colunas derivadas de uma coluna da tabela mantêm o nome dela com o marcador de período (`net_amount_ref`, `return_amount_prev`, `var_net_amount`). O dashboard é a camada de apresentação, e o idioma é o do usuário. As colunas que vêm das tabelas mantêm o nome original.
- **A convenção de nomes em inglês** ([convenções](../convencoes.md)) vale para tabelas e colunas do lakehouse.

## Achado durante a revisão: vazamento de futuro no simulador

- **Como apareceu:** ao medir o viés de maturação (D3), o dia 2026-10-26 visto em 2026-11-01 tinha perdido só 2 pedidos em relação ao visto em 2026-10-27, contra ~34 esperados.
- **Causa:** a função `order_status_as_of`, do gerador da carga inicial, que o simulador diário usa, marcava CANCELADO o pedido com cancelamento antes do faturamento (`cancela_antes`) sem comparar `dt_cancela` com a data da foto.
  - O cancelamento de D + 1 vazava para a extração E = D + 1, com `dt_atualizacao` em E.
  - A extração seguinte trazia o cancelamento de novo, agora corretamente.
- **Medido na bronze:** linhas de `pedidos_cab` com data depois de D, todas CANCELADO. Nunca mais de 1 dia à frente.

| Extração | Linhas vazadas | Extração | Linhas vazadas |
|---|---|---|---|
| 2026-10-20 | 27 | 2026-10-27 | 26 |
| 2026-10-21 | 36 | 2026-10-28 | 23 |
| 2026-10-22 | 23 | 2026-10-29 | 23 |
| 2026-10-23 | 20 | 2026-10-30 | 21 |
| 2026-10-24 | 39 | 2026-10-31 | 16 |
| 2026-10-25 | 5 | 2026-11-01 | 8 |
| 2026-10-26 | 0 | **Total** | **267** |

  - Carga inicial (2026-10-19): 0. Outros datasets incrementais (itens de pedido, NFs e itens de NF): 0.
- **Efeito:**
  - o estado final é correto: o `AUTO CDC` absorve a repetição;
  - a foto do D-1 já saía sem esses cancelamentos de D + 1. Isso é irrealista e escondia parte do viés do D3;
  - contraria o contrato do ADR-0012: "a extração E traz o que mudou em D = E − 1".
- **Correção** (`data-engineer`): `cancelado = (cancela_antes AND dt_cancela <= as_of) OR cancelado_depois`.
  - Fica na função compartilhada, e não só no laço do simulador, para a função não mentir e a regra não ficar duplicada.
  - A *docstring* registra a **regra de corte:** todo evento entra na foto só a partir da sua data. Até `dt_cancela`, o pedido está ABERTO.
  - **Efeito na carga inicial: zero.** Nenhum pedido de 2026-10-18 tem `dt_cancela` em 2026-10-19 (0 linhas na bronze de 2026-10-19 e 0 no oráculo local).
- **Validação:**
  - **oráculo local** (PySpark 3.5.5, `faker==37.12.0`): com o código antigo, reproduz a bronze **exatamente**, nas linhas e nos vazamentos de cada extração. Com o novo, dá 0 vazamento. A única diferença são os pedidos vazados, que passam a chegar ABERTO;
  - **previsão:** a extração de 2026-11-02 não distingue os códigos, porque D é domingo. A primeira que prova a correção é a de 2026-11-03 (449 linhas; o código antigo vazaria 3);
  - **a confirmação real depende das próximas execuções**, cada uma com o ok do usuário.
- **As extrações já gravadas não são regeradas** (regra da landing, ADR-0012 D4). Consequências:
  - o conteúdo de cada extração passa a depender também da versão do gerador, além da data, da versão do Faker e do ambiente (ADR-0012 D3 e D9);
  - um reenvio (`--resend`) de uma data até 2026-11-01 sairia diferente do original nos pedidos vazados, que viriam ABERTO. Na silver, é inofensivo: o `SEQUENCE BY` mantém o mais novo. Mas o reenvio deixaria de ser idêntico;
  - **provas de reenvio só com datas a partir de 2026-11-02.**
- **Não é decisão nova:** é correção de implementação, que restabelece o contrato do ADR-0012. Por isso entra só na linha de status dele.

## Free Edition: o que a documentação diz e o que foi confirmado

| Recurso | Documentação oficial | No projeto |
|---|---|---|
| Dashboards AI/BI | A [página do Free Edition](https://docs.databricks.com/aws/en/getting-started/free-edition) os menciona só indiretamente. A de [limitações](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations) não fala deles | **Confirmado na prática:** implantado pelo bundle, publicado e renderizado (2026-11-01) |
| Limites do dashboard | 15 páginas, 100 datasets e 100 widgets por página. O filtro roda no navegador até 100 mil linhas e 100 MB por dataset ([limites](https://docs.databricks.com/aws/en/dashboards/limits)) | 2 páginas e 9 datasets; o maior tem 11.211 linhas (D7) |
| `resources.dashboards` no bundle | CLI ≥ 0.232.0 ([recursos do bundle](https://docs.databricks.com/aws/en/dev-tools/bundles/resources)) | Em uso |
| `dataset_catalog` e `dataset_schema` | CLI ≥ 0.283.0. Um catálogo e um schema por dashboard; o nome qualificado no SQL vence o padrão ([recursos do bundle](https://docs.databricks.com/aws/en/dev-tools/bundles/resources), [API](https://docs.databricks.com/api/lakeview/v1/dashboard)) | `core` / `gold` (D9) |
| `${var.x}` dentro do `.lvdash.json` | Não documentado | **Testado:** não interpola (D9) |
| `serialized_dashboard` *inline* | A interpolação e o `bundle generate` com ele não são documentados. Inferência: o `generate` escreve um `.lvdash.json` e um `file_path` | Não usado (D9) |
| `parent_path` sem valor | Não documentado | **Confirmado na prática:** o dashboard fica na pasta do bundle (D12) |
| Edição pela interface, `bundle generate` e `deploy --force` | [Recursos do bundle](https://docs.databricks.com/aws/en/dev-tools/bundles/resources), [comandos do bundle](https://docs.databricks.com/aws/en/dev-tools/cli/bundle-commands) | D12 |
| Cache do dashboard publicado | Até 24 h, *best-effort*. Uma atualização agendada é o único jeito confiável de renová-lo. `current_timestamp()` não o invalida. Com dados compartilhados (credencial do publicador), o cache é compartilhado; com credenciais individuais, é por usuário. **Não documentados:** de quando contam as 24 h (leitura provável: da obtenção do resultado) e a granularidade do cache ([caching](https://docs.databricks.com/aws/en/dashboards/caching)) | D4, D10 e D11 |
| Credencial embutida | "*might expose data to users who have not been granted direct access*" ([compartilhar dashboards](https://docs.databricks.com/aws/en/dashboards/share/share)) | Risco aceito (D11) |
| Tarefa de dashboard no job | Atualiza um dashboard publicado; `dashboard_task` no bundle com CLI ≥ 0.248.0; sem `warehouse_id`, usa o do dashboard ([tarefa de dashboard](https://docs.databricks.com/aws/en/jobs/tasks/dashboard), [tipos de tarefa no bundle](https://docs.databricks.com/aws/en/dev-tools/bundles/job-task-types)). O Free Edition não é mencionado | **Confirmado na prática:** SUCCESS na 1ª tentativa, em 42 s, e o cache compartilhado foi atualizado: o painel publicado mostrou o dia novo sem atualização manual (2026-11-01) |
| Assinatura por e-mail | [Agendamento e assinatura](https://docs.databricks.com/aws/en/dashboards/share/schedule-subscribe); não confirmada no Free Edition | Não usada. Teste na fatia 2, com o da notificação de falha do job |
| Padrão dinâmico de data em parâmetro | Documentado só para **intervalo** de datas (`now-1d/d`...), e seria o relógio ([parâmetros](https://docs.databricks.com/aws/en/dashboards/manage/filters/parameters)) | Não usado (D2) |
| *Metric views* | Exigem warehouse ou compute com DBR 17.3+ ([criar metric views](https://docs.databricks.com/aws/en/metric-views/create/)). Disponibilidade no Free Edition **não confirmada**; o comportamento de `COUNT DISTINCT` sob os filtros do dashboard não é documentado | Não usadas (D7) |
| Funções HyperLogLog | Existem no Databricks SQL, DBR 13.3+ ([`hll_sketch_agg`](https://docs.databricks.com/aws/en/sql/language-manual/functions/hll_sketch_agg), [`hll_union_agg`](https://docs.databricks.com/aws/en/sql/language-manual/functions/hll_union_agg), [`hll_sketch_estimate`](https://docs.databricks.com/aws/en/sql/language-manual/functions/hll_sketch_estimate)). O uso em dashboard e o erro não são documentados; Free Edition não confirmado | Caminho para 100x, não decidido |

## Resultados

### Validação no deploy (data padrão antes da execução do job: segunda 2026-10-26 × segunda 2026-10-19)

| Indicador | 2026-10-26 | Contra 2026-10-19 |
|---|---|---|
| Venda líquida | R$ 11.976.134,57 | −3,5% |
| Volume | 12.383,92 hL | −4,1% |
| Pedidos | 2.454 | +2,3% |
| Faturado líquido | R$ 4.438.234,58 | −17,0% |
| Devoluções | −R$ 13.156,39 | −R$ 2.136,56 (+19,4% de devolução; eram −R$ 11.019,83) |

- **Frescor:** dados até 2026-10-26; carregados em 2026-10-27 07:29 (Brasília).
- **Totais conferidos, nas duas páginas, com o `-1` incluído:** fato = indicador = por região = por marca = por canal = tabela = série.
- **Membro `-1`:** R$ 1.823,12 em canal DESCONHECIDO, nos dois fatos; 0 em região e em marca (D8).
- **Tempo:** 0,8 a 2,2 s por dataset a frio, 0,3 a 0,6 s com cache e ~19,8 s com o warehouse parado (D6).

### Deploy (2026-11-01, `databricks bundle deploy`)

- O dashboard foi criado e publicado, com `embed_credentials: true`.
- Os 9 datasets ficaram com catálogo `core` e schema `gold`.
- O job recebeu o UUID do dashboard na `refresh_dashboard`. A referência `${resources.dashboards.commercial_sales.id}` resolve no deploy; o `bundle validate` não confere referências.
- O agendamento do job continua pausado (ADR-0005).
- O usuário abriu as duas páginas: todos os widgets renderizaram, e os números bateram.

### Execução do job com a atualização (2026-11-01, run `520956708131314`)

Etapa 2 do fechamento da fatia, com o ok do usuário. Parâmetros padrão: o *catch-up* gravou as extrações que faltavam. Ainda com o código do deploy: sem o filtro geográfico, com 0 retentativas na atualização e com o simulador antigo.

| Tarefa | Resultado | Tempo |
|---|---|---|
| `simulate_erp_extraction` | 5 extrações, de 2026-10-28 a 2026-11-01 | 217 s |
| `core` | SUCCESS | 163 s |
| `commercial` | SUCCESS | 102 s |
| `verify_uniqueness` | As 10 contagens = 0. É a primeira vez no job com as 3 contagens novas do ADR-0012 D7 | 39 s |
| `refresh_dashboard` | SUCCESS na 1ª tentativa | 42 s |
| **Job** | **SUCCESS** | **~9,5 min** |

- **Free Edition:** a tarefa de dashboard (`dashboard_task`) funciona, confirmado na prática. A assinatura por e-mail segue não confirmada.
- **Bronze:** cresceu exatamente o que foi gravado, nos 7 datasets. Exemplos: itens de pedido +37.624; clientes +365.141 (73.001 + 4 × 73.035).
- **Silver:** itens de pedido +37.578, ou seja, a bronze menos 46 itens descartados por `positive_quantity_and_amount` (sujeira intencional). Nas NFs, silver = bronze.
- **Fatos:** D-1 = 2026-10-31 nos dois. `fact_sales` com 3.025.368 linhas e `fact_invoice` com 3.111.348.
- **Cliente `-1`: 0 nos dois fatos** (eram 254 e 256 em 2026-10-27). Todos os clientes fora da carga chegaram no cadastro.
- **Dimensões:** `dim_customer` passou de 47.387 para 47.430 linhas. `dim_sales_rep`, `dim_product` e `dim_date` ficaram com checksum idêntico: depois do go-live, o simulador não cria cadastro novo nem reorganização (ADR-0012 D1).
- **Expectations:** no `core`, só `document_valid` (1.338 avisos, como sempre) e os 46 descartes acima. No `commercial`, nenhuma violação.
- **Dataset de frescor do dashboard:** referência "31/10/2026 (sábado)", comparada com "24/10/2026 (sábado)"; dados até 2026-10-31; carregados em 2026-11-01 09:40 (Brasília).
- **Vendas em 2026-10-31 (sábado):** venda líquida R$ 3.396.078,09, contra R$ 2.862.283,47 em 2026-10-24 (+18,6%); 742 pedidos; 3.882,81 hL.
- **Cache compartilhado atualizado pela tarefa: confirmado.** De manhã, o usuário tinha aberto o dashboard publicado, com dados até 2026-10-26. Depois da execução, e antes de qualquer deploy novo, ele o abriu de novo e viu "Dados até 31/10/2026" sem atualizar à mão. É a prova de que a `refresh_dashboard`, com `embed_credentials: true`, atualiza o cache que o visualizador lê (D10, D11).

### Deploy dos ajustes da revisão (2026-11-01, 13:45 UTC)

- **Simulador corrigido** no workspace, conferido no arquivo implantado. A confirmação em execução real vem nas extrações de 2026-11-02 e 2026-11-03 ("Achado durante a revisão").
- **Job:** `refresh_dashboard` com 1 retentativa e 60 s de intervalo (D10). O agendamento continua pausado.
- **Dashboard republicado**, com 5 filtros por página: Data de referência, Região comercial, Região geográfica (cliente), Marca e Canal (D13). `embed_credentials: true`.
- **Filtro geográfico (D13):** linhas iguais antes e depois em todos os datasets; teste do NORDESTE e do SUDESTE no D13.
- **Observação: republicar o dashboard zera o cache.** Por isso a confirmação visual foi feita antes deste deploy. A primeira visualização depois de um deploy consulta o warehouse.

### Ainda não confirmado

- **A correção do simulador em execução real:** a primeira extração que a prova é a de 2026-11-03.
- **A assinatura por e-mail:** não usada e não confirmada no Free Edition. Teste na fatia 2.

## Alternativas consideradas

| Alternativa | Por que não |
|---|---|
| D1: só o faturamento | O atraso entre o pedido e a NF esconde o "ontem". Na segunda 2026-10-26, o faturado foi R$ 4,4 mi, contra R$ 12,0 mi de venda |
| D1: só a venda, sem a página 2 | O `fact_invoice` ficaria sem consumidor visível na fatia que o criou, e a confusão entre vendido e faturado que o ADR-0003 previu continuaria fora do painel |
| D1: somar venda e faturamento | Contagem dupla: o mesmo item de pedido aparece de novo quando é faturado |
| D2: parâmetro de data com padrão relativo | O padrão dinâmico só é documentado para **intervalo** de datas (`now-1d/d`...), e seria o relógio. Com o job atrasado, o painel mostraria um dia vazio ou incompleto |
| D2: data padrão fixa | Envelhece: no dia seguinte, o padrão já não é o último dia |
| D2: só a última data, sem filtro | Tira do usuário a escolha de outro dia |
| D3: comparar com o dia anterior (referência − 1) | A venda varia muito com o dia da semana: segunda contra domingo diria pouco |
| D3: comparar com o que se sabia do dia referência − 7 no seu próprio D + 1 | Tiraria o viés de maturação, mas exige reconstruir o estado "*as-of*" da bronze a cada consulta. YAGNI: o viés é de ~1,5 p.p., medido e avisado no painel |
| D4: "atraso em dias" calculado com `current_date` | Sob o cache de 24 h, seria o atraso da hora da consulta, e não o da visualização. Poderia dizer "em dia" com dado velho |
| D6: tabela agregada no `commercial.gold` | YAGNI: 0,8 a 2,2 s por dataset a frio, longe do gatilho de ~10 s |
| D7: indicadores de vendas sem o pedido no grão | "Pedidos" somaria entre marcas: 5.705 contra 2.454 reais em 2026-10-26 |
| D7: *metric view* para a contagem distinta governada | Disponibilidade no Free Edition não confirmada (exige DBR 17.3+), e o comportamento de `COUNT DISTINCT` sob os filtros do dashboard não é documentado. O roadmap já põe as *metric views* na fatia 4 |
| D8: `JOIN` interno nas dimensões | Uma `_key` sem linha na dimensão sairia do total, e o painel deixaria de bater com o fato |
| D9: catálogo padrão = `commercial` | Três literais `core.gold.dim_*` por consulta, em vez de um |
| D9: parâmetro oculto com `IDENTIFIER()` | O nome só muda de lugar: o valor padrão do parâmetro continua no JSON |
| D9: views de consumo no `commercial.gold`, agora | Documentação contraditória sobre publicar view por pipeline, sem confirmação no Free Edition; uma materialized view larga materializaria ~3 mi de linhas por execução; uma view de setup voltaria a escrever o catálogo (D9). É o caminho de produção |
| D9: JSON gerado por target | Código de build a mais, sem um segundo target hoje |
| D9: `serialized_dashboard` *inline* no YAML | Seria interpolado (inferência; não documentado), mas perde o `.lvdash.json` e o `bundle generate dashboard`, e põe ~2.700 linhas de JSON no YAML |
| D10: sem atualização (o plano inicial) | Com o cache de 24 h, às 8h o diretor poderia ver o dia anterior ao D-1 |
| D10: agendamento do próprio dashboard | Desacoplado do job: atualizaria antes de o job terminar ou depois de uma falha |
| D10: atualizar logo depois do `commercial`, antes da verificação | Gastaria o warehouse para publicar no painel um dia que a verificação pode acusar |
| D10: 0 retentativas | Uma falha passageira na partida a frio do warehouse deixaria o job vermelho e o painel sem o dia novo |
| D10: assinatura (`subscription`) por e-mail | Entrega não confirmada no Free Edition |
| D11: `embed_credentials: false` | No cenário que o painel modela, o diretor não publica, e a atualização feita pelo "Run as" não chegaria a ele |
| D12: warehouse pelo id | O id muda se o warehouse for recriado |
| D12: editar pela interface | O workspace diverge do Git, e o deploy falha até alguém escolher um lado |
| D12: sincronizar o `.lvdash.json` | Viraria uma segunda cópia do dashboard no workspace |
| D13: agregar a tabela de combinações no widget, sem a região geográfica no grão do dataset | Exigiria uma medida para a variação %, e o suporte não foi verificado |
| Simulador: regerar as extrações já gravadas com o código corrigido | Contraria a regra da landing (ADR-0012 D4). No mesmo caminho, o arquivo já processado nunca é reingerido; num caminho novo, a bronze guardaria as duas cópias. E o estado final já está certo: o `AUTO CDC` absorveu a repetição |

## Consequências

- **Positivas:**
  - o diretor tem a resposta por região comercial, marca e canal, com a última data carregada e a comparação com a semana anterior; a região geográfica do cliente fica como filtro secundário (ADR-0006 §7);
  - o total do painel bate com o fato, com o `-1` incluído e rotulado;
  - venda e faturamento têm página, rótulo e definição próprios (fecha a dívida do ADR-0003);
  - o painel nunca mostra um dia vazio por atraso do job, e o frescor diz qual dia está na tela;
  - o dashboard é código: o deploy o recria, e o job o atualiza;
  - nenhum objeto novo na gold;
  - a revisão achou e corrigiu um vazamento de futuro no simulador.
- **Negativas / dívidas assumidas:**
  - **catálogo do domínio escrito no JSON** (D9): se `catalog_commercial` mudar, o painel continua lendo o catálogo antigo, em silêncio. A guarda é só um comentário no `databricks.yml`;
  - **`_key` sem linha na dimensão** cai no DESCONHECIDO, sem aviso, e a `verify_uniqueness` não confere isso (D8). Gatilhos para uma contagem: o `done-checker` achar algum caso; o `core` rodar fora do job; uma fonte real, com versões que mudam;
  - **a atualização depois da verificação não protege o painel** com o job diário (D10). A verificação segue sem aparecer para o consumidor até a fatia 2;
  - **as contagens 8 a 10 e o job vermelho** (dívida do ADR-0012): um caso real de mesmo início vindo da fonte deixaria o job vermelho todo dia, e a `refresh_dashboard` seria pulada todo dia. O painel seguiria consultando o warehouse quando o cache expirasse;
  - **viés de maturação** (D3): a variação sai ~1,3 a ~1,5 p.p. mais alta. Medido e avisado, não corrigido;
  - **"Dados até" diz o dia, e não se o dia está completo.** Uma extração atrasada ou parcial deixa o D-1 incompleto sem aviso (ADR-0010, Perguntas difíceis). Detectar fonte que não chegou é tema da fatia 2;
  - **"Carregados em"** não muda numa recarga e avança num reenvio sem dado novo (D4);
  - **frescor e indicadores** podem, raramente, vir de gerações diferentes do cache (D4);
  - **feriado na semana anterior distorce a comparação**, e a `dim_date` ainda não tem feriados (D3);
  - **meta sem fonte e sem dona** (D5);
  - **credencial embutida** (D11): ela lê tudo o que o publicador lê, inclusive a bronze com PII. O que protege é o JSON fixo;
  - **edição só pelo JSON** (D12): uma edição pela interface trava o deploy até alguém escolher um lado;
  - **um único warehouse 2X-Small**, disputado pelo dashboard, pela atualização e pelas consultas *ad-hoc*. A partida a frio leva ~19,8 s;
  - **o filtro geográfico é quase redundante** nos dados sintéticos (D13);
  - **as extrações de 2026-10-20 a 2026-11-01 ficam com o vazamento** (267 linhas). Um reenvio dessas datas não sai idêntico ao original; provas de reenvio, só a partir de 2026-11-02;
  - **CLI ≥ 0.283.0** para implantar (`dataset_catalog`);
  - **a assinatura por e-mail não foi confirmada** no Free Edition. O teste fica para a fatia 2, junto com o da notificação de falha do job;
  - **republicar o dashboard zera o cache** (observado em 2026-11-01): a primeira visualização depois de cada deploy paga a consulta no warehouse.
- **O que nos faria revisitar:**
  - um dataset passar de ~10 s (D6), ou um dataset passar de 100 mil linhas (D7);
  - a fonte de metas existir (D5);
  - um segundo target ou ambiente (D9);
  - mais de um usuário, ou um visualizador que não seja quem publica (D11);
  - a `refresh_dashboard` falhar, ou o painel publicado não mostrar o dia novo depois dela (D10);
  - a confirmação da assinatura por e-mail no Free Edition;
  - o *write-audit-publish* ser implementado: o dashboard passaria a ler a última versão auditada (ADR-0012);
  - a extração de 2026-11-03 trazer um cancelamento de D + 1 (a correção do simulador não pegou);
  - um widget passar a comparar canal, segmento ou região geográfica entre os dois fatos: aí vale refazer a medição do ADR-0010 D1b. Hoje cada página mostra os atributos do cliente do seu fato, sem comparação entre eles.

## Perguntas difíceis

- **"E se o job não rodar, ou o arquivo chegar 2 dias atrasado?"** O painel mostra a última data carregada, e "Dados até" diz qual é. Não aparece um dia vazio. Não há alerta: detectar fonte que não chegou é tema da fatia 2. Quando o arquivo chega, os fatos recalculados põem as linhas na data de negócio certa (ADR-0010).
- **"E se a verificação falhar?"** A `refresh_dashboard` não roda. Com o job diário, isso não protege o painel: o cache do dia anterior já tem ~24 h, e a visualização das 8h consulta o warehouse e mostra o dado novo, possivelmente duplicado. A verificação é alarme, e não portão; tornar o alarme visível no painel é tema da fatia 2 (D10).
- **"Por que não `current_date - 1`?"** Porque é o relógio, e não o dado. Com o job atrasado, o painel mostraria um dia vazio ou incompleto (D2).
- **"Por que o painel não mostra 'atraso em dias'?"** Porque o resultado fica em cache por até 24 h. O atraso seria o da hora da consulta, e o painel poderia dizer "em dia" com dado velho (D4).
- **"Por que a venda e o faturamento de segunda não batem?"** São eventos diferentes, em datas diferentes. O faturamento atrasa o pedido de 0 a 2 dias, e só ~38% dos pedidos de segunda são faturados no mesmo dia. A parte atrasada que chega na segunda vem do fim de semana, que vende pouco: R$ 4,4 mi faturados contra R$ 12,0 mi vendidos em 2026-10-26 (D1).
- **"A variação de hoje não está otimista demais?"** Está, em ~1,3 a ~1,5 p.p. O dia de referência ainda vai perder cancelamentos; o de comparação já perdeu. O painel avisa (D3).
- **"Por que os pedidos por marca não somam o total?"** Porque um pedido com duas marcas conta uma vez em cada uma. A contagem distinta não é aditiva: 5.705 somando por marca, contra 2.454 pedidos (D7).
- **"Por que dois filtros de região?"** A região comercial é a das metas; a geográfica é a pergunta de mercado (ADR-0006 §7). Nos dados sintéticos, elas só diferem em CONTAS NACIONAIS (D13).
- **"Como um simulador vaza o futuro?"** Uma foto "*as-of*" que não compara a data de cada evento com a data da foto. Aqui, o cancelamento de amanhã entrava na extração de hoje. A defesa é uma regra de corte única, na função compartilhada, e um oráculo que reproduz a bronze ("Achado durante a revisão").
- **"O diretor vê o D-1 às 8h?"** Com o agendamento ativo (05:00), sim. A execução de 2026-11-01, com 5 extrações de *catch-up* e a atualização, levou ~9,5 min, e o painel publicado mostrou o dia novo sem atualização manual (confirmado em 2026-11-01). Hoje o agendamento está pausado para poupar a cota (ADR-0005), e o D-1 depende de rodar o job à mão.
- **"Qual o custo em 10x?"** Ver "Em produção".

## Em produção faríamos diferente?

Sim:
- **Vários ambientes: views de consumo no catálogo do domínio.** O dashboard lê views em `commercial` que juntam fatos e dimensões. O literal `commercial.gold` some do JSON, e as views convergem com o *write-audit-publish* do ADR-0012: elas apontam para a última versão auditada (D9).
- **Identidade:** `embed_credentials: true` com um *service principal* de menor privilégio (`SELECT` só em `commercial.gold` e `core.gold`), e os visualizadores num grupo com `CAN VIEW`. A credencial não alcança a PII nem se o JSON mudar (D11).
- **Portão:** *write-audit-publish* nos fatos do SLA das 8h. A atualização do dashboard só roda depois da promoção (ADR-0012).
- **Agendamento ativo**, com notificação de falha, assinatura do painel, alerta de frescor e o resultado da verificação visível no painel (fatia 2).
- **Metas:** uma fonte de metas com contrato e com dona, e o grão dela decidindo a junção (D5).
- **Warehouse:** um warehouse para o BI, separado das consultas *ad-hoc*. O Free Edition tem um só, 2X-Small.
- **Simulador:** não existe; o ERP entrega os arquivos. Mas a lição vale para qualquer gerador de dados de teste: regra de corte "*as-of*" única e um oráculo.

**Em 10x** (~30 mi de itens em cada fato). Extrapolação, não medição:
- se o tempo de leitura crescer com o volume, os 0,8 a 2,2 s por dataset passam do gatilho de ~10 s. Entra a tabela agregada por dia × região × marca × canal, ou o *liquid clustering* por data nos fatos (fatia 4);
- o dataset dos indicadores de vendas, com o pedido no grão, passaria de ~11 mil para ~110 mil linhas em 2 dias. Acima do limite de 100 mil linhas, **o filtro vai para o warehouse:**
  - a contagem continua exata;
  - mas cada clique vira uma consulta no 2X-Small compartilhado, com partida a frio de ~19,8 s;
  - é o caminho intermediário entre o navegador e o HyperLogLog.

**Em 100x** (~300 mi de itens em cada fato). Extrapolação, não medição:
- a tabela agregada é obrigatória e precisa de refresh incremental, como os fatos (ADR-0010);
- o dataset dos indicadores com o pedido no grão chegaria a ~1,1 mi de linhas em 2 dias;
- **caminho para os pedidos distintos, não decidido:** guardar um *sketch* HyperLogLog por célula do grão (`hll_sketch_agg`) e uni-los sob os filtros (`hll_union_agg` + `hll_sketch_estimate`), aceitando uma contagem aproximada. As funções existem no Databricks SQL (DBR 13.3+). O uso em dashboard, o erro da estimativa e a disponibilidade no Free Edition ficam *a verificar* quando chegar lá.

## Conceitos para estudar

- métrica aditiva × não aditiva: por que a contagem distinta não se soma entre linhas;
- contagem distinta aproximada: *sketches* HyperLogLog, que se unem sem recontar, e o erro que eles aceitam;
- dependência funcional: por que uma coluna nova não aumenta o grão (pedido → cliente → região);
- tempo do evento × tempo de processamento × relógio: "a última data carregada" contra `current_date - 1`;
- maturação do dado: comparar dias de idades diferentes enviesa a variação;
- vazamento de futuro (*look-ahead*): regra de corte "*as-of*" e oráculo que reproduz a saída;
- cache de BI e invalidação: por que o frescor tem de vir do dado, e não da hora da consulta;
- sazonalidade semanal e comparação com o mesmo dia da semana anterior;
- membro desconhecido no consumo e integridade referencial fato → dimensão;
- dashboards como código: bundle, divergência entre workspace e Git, `bundle generate` e `deploy --force`;
- credencial embutida × credencial individual; *service principal*; menor privilégio;
- atualização como etapa do DAG × agendamento desacoplado; alarme × portão (*write-audit-publish*);
- quando criar um agregado: gatilho medido, e não palpite (YAGNI);
- camada semântica (*metric views*), onde a definição de "pedidos" ficaria governada;
- camada de apresentação: nomes no idioma do usuário, sem mudar o contrato das tabelas.
