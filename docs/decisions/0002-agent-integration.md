# ADR 0002: bounded agent stages and durable model operations

Status: accepted for milestone 4.

## Decision

Implement a small versioned agent runtime directly over the existing executor.
LangGraph and other agent frameworks remain possible later integrations. The
current workflow needs bounded planning, extraction, evidence review, and writing;
adding a second workflow/checkpoint owner would complicate recovery without adding
necessary behavior for this scope.

The server admits an immutable template DAG with one planner, one to four research
branches, one reviewer, and one writer. The leased model-backed planner chooses
research objectives and source assignments. It does not execute in the admission
transaction, mutate the admitted DAG, generate code, or select arbitrary handlers.
Dynamic graph revision remains deferred.

Every generation goes through the shared budget gateway. Prompt version plus
bounded step index generates each logical call key. Source documents, parent
outputs, model configuration, and price card are frozen in durable state. Repeating
the same logical call returns its recorded response; changed context conflicts.

Use the OpenAI Responses HTTP API with strict structured outputs, input-token
preflight, an explicit output cap, no tools, default service tier, and a configured
model/rate card. No model or current price is guessed. This adapter is opt-in.

Responses run in background mode with stored responses. Persist the response ID
as soon as it arrives and use GET for recovery. Never repeat an uncertain create.
A lost create response without an ID retains its reservation until an operator
can associate a response whose metadata matches the operation ID and request hash.
This deliberately favors retained liabilities over duplicate charges.

Provider reconciliation runs separately from scheduling in Compose. Network I/O
never holds a run-row transaction open. Local standalone mode may combine loops
for development, with explicit configuration.

The recorded backend uses packaged, hand-authored synthetic response fixtures for
an exact demonstration request/corpus. It exercises the same agents, source
validation, accounting, and report renderer; it is not evidence that a live LLM
was called. Mocked HTTP tests verify the real adapter's payloads and recovery.

## Evidence guarantees and limits

Research references only owned source documents. Exact quotes and character
locators are checked, and claim IDs are assigned by the runtime. The reviewer
and writer may reference only known claims. Reports always preserve unresolved
review conflicts and include source links. Structured/semantic output errors
allow one separately budgeted repair call, then fail permanently.

Quote existence is a provenance check, not a proof of entailment or factual truth.
Source quality and model interpretation require evaluation in milestone 6. No
external search, arbitrary tool execution, or long-term memory is enabled here.

## Official API references consulted

- [Structured Outputs](https://developers.openai.com/api/docs/guides/structured-outputs)
- [Input token counting](https://developers.openai.com/api/docs/guides/token-counting)
- [Background responses and retrieval](https://developers.openai.com/api/docs/guides/background)

Provider availability, supported models, and prices remain deployment-specific.
