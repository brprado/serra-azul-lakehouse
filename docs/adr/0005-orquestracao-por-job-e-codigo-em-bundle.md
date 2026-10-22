# ADR-0005: Orquestrar os pipelines por um Job e versionar tudo em Git com um bundle

- **Status:** aceito — emendado pelo [ADR-0010](0010-pipeline-commercial-fatos-chaves-e-membro-desconhecido.md) (D13: tarefa final de verificação de unicidade no job `serra_azul_daily`)
- **Data:** 2026-10-19
- **Fatia:** fatia 1 (revisão da fase 0)
- **Decisor:** Engenheiro de Dados Sênior (Claude) · **Acompanhamento:** usuário (pode vetar)
- **Relação:** emenda o ADR-0002. Continuam valendo o SDP em SQL, o pipeline `core` mais um pipeline por domínio e a parametrização `${catalog_*}`.

## Contexto

O ADR-0002 escolheu o SDP com **disparo manual** e pipelines **configurados à mão**. Ele mesmo registrou as dívidas: o SLA de 8h não é garantido e não existe um ponto único de configuração. Além disso, o projeto não tem controle de versão.

As três são práticas básicas: custam pouco no começo e muito depois, quando já existem pipelines, job e dashboard criados pela interface sem histórico.

A página oficial de limitações do Free Edition não restringe bundles nem agendamento de jobs (consultada em 2026-10-19). Um bundle já foi implantado com sucesso no Free Edition neste projeto (fase do guia, perfil `aws`).

## Decisão

**Todo o código vai para Git; pipelines, job e dashboard são descritos num bundle; um Job encadeia a execução diária.**

1. **Git.** A pasta do projeto vira um repositório, que versiona:
   - o SQL dos pipelines;
   - os notebooks de simulação;
   - o bundle;
   - os ADRs;
   - o setup.
2. **Bundle** (Declarative Automation Bundles, arquivo `databricks.yml`). Ele descreve os pipelines, o job e o dashboard como código, e a implantação é feita com `databricks bundle deploy --profile aws2`. As variáveis `catalog_core` e `catalog_commercial` vão para a `configuration` de cada pipeline. A regra `${catalog_*}` do ADR-0002 continua, agora preenchida num lugar só.
3. **Job `serra_azul_daily`.** As tarefas rodam em série:
   1. `simulate_erp_extraction`: notebook que grava na landing o extrato do ERP de D-1;
   2. pipeline `core`;
   3. pipeline `commercial`.
   
   Detalhes do agendamento:
   - **Horário:** 05:00 de `America/Sao_Paulo`, com 3h de folga para o SLA das 8h.
   - **Retry:** 1 por tarefa.
   - **Pausa:** o agendamento fica **pausado** no projeto. As execuções são disparadas à mão com `databricks bundle run`, porque cada execução consome a cota.
4. **Validação sem custo.** Antes de executar, rodam `databricks bundle validate` e uma atualização *validate-only* dos pipelines. Ela confere o SQL e o grafo de dependências sem processar dados.
5. **Um único target**, o workspace `aws2`. O dev fica a uma alteração de distância: basta um novo target com outras variáveis de catálogo.
6. **Catálogos e schemas** continuam em `setup/*.sql`, porque no Free Edition a API não cria catálogos. O bundle só os referencia.

## Alternativas consideradas

| Alternativa | Por que não |
|---|---|
| Manter o disparo manual (ADR-0002) | A ordem `core` → `commercial` dependeria da memória de alguém, não haveria histórico por execução e o SLA não teria como ser cumprido |
| Um único pipeline com todas as camadas e domínios | Eliminaria a orquestração entre pipelines, mas o dono do código deixaria de ser o dono do dado (ADR-0001). Além disso, uma falha no comercial bloquearia o `core`, do qual todos os domínios dependem. Fica como plano B se o Free Edition permitir só um pipeline ETL |
| Configurar pipelines e job pela interface | Sem histórico, sem revisão e sem reprodução: refazer o ambiente vira arqueologia |
| Ambiente de dev separado (target + catálogos `*_dev`) | É a prática certa em produção, mas aqui dobraria o consumo da cota, cujo estouro pode desligar o compute pelo resto do dia ou do mês |
| Agendamento ativo todo dia | Consumiria cota mesmo nos dias sem estudo. A conta `aws` já foi bloqueada uma vez |

## Consequências

- **Positivas:**
  - o ambiente se reproduz com um comando;
  - toda mudança tem histórico e diff;
  - a ordem de execução é garantida pelo job, qualquer que seja o significado de "pipeline ativo" no Free Edition;
  - o SLA passa a depender só de despausar o agendamento.
- **Negativas / dívidas assumidas:**
  - há a curva de aprendizado do bundle;
  - os catálogos ficam fora dele, criados por SQL;
  - sem dev, um erro chega direto ao "prod". A mitigação é o *validate-only* e as tabelas idempotentes.
- **A verificar na implementação:**
  - se o Free Edition permite notificação de falha por e-mail;
  - se a atualização *validate-only* funciona por bundle e CLI.
- **O que nos faria revisitar:**
  - a cota permitir rodar dois ambientes;
  - o volume tornar longa demais a janela sequencial `core` → domínios.

## Em produção faríamos diferente?

Sim:
- **Ambientes:** targets `dev`/`staging`/`prod`, cada um com seus catálogos. Outra opção são workspaces separados com *workspace-catalog binding*.
- **CI:** um pipeline de CI, como GitHub Actions, rodaria `bundle validate` e testes em cada PR.
- **Deploy:** em `prod`, só pelo CI e com *service principal* (o job roda como identidade técnica, não como pessoa).
- **Agendamento:** ativo, com alerta.

**Em 10x de volume:** a janela sequencial cresce, e os pipelines de domínio passariam a rodar em paralelo depois do `core` (fora do Free Edition).

**Em 100x:** o próprio `core` precisaria ser dividido por fonte, para que uma fonte lenta não atrase as outras.

## Conceitos para estudar

- infraestrutura como código e Declarative Automation Bundles (targets, variáveis, modo de desenvolvimento);
- orquestração com Lakeflow Jobs (`pipeline_task`, dependências, retries);
- SLA vs frequência de execução;
- *validate-only* / *dry run*;
- promoção de código entre ambientes;
- *service principal* e `run_as`.
