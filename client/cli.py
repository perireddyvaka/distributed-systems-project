"""Command-line interface (CLI) for the Distributed Search Engine.

Supports document CRUD operations, distributed scatter-gather search,
cluster health status inspection, and replica recovery triggering.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import click
from client.client import SearchEngineClient


@click.group()
@click.option(
    "--coordinator",
    default="127.0.0.1:50050",
    help="Target coordinator address (host:port)",
    show_default=True,
)
@click.pass_context
def cli(ctx, coordinator: str) -> None:
    """Distributed Search Engine Command Line Interface."""
    ctx.ensure_object(dict)
    ctx.obj["coordinator"] = coordinator


@cli.command()
@click.option("--query", "-q", required=True, help="Search query string")
@click.option("--top-k", "-k", default=10, help="Number of top results to retrieve", show_default=True)
@click.pass_context
def search(ctx, query: str, top_k: int) -> None:
    """Execute a distributed scatter-gather BM25 search across all shards."""
    target = ctx.obj["coordinator"]
    client = SearchEngineClient(target)
    try:
        resp = client.search(query=query, top_k=top_k)
        if resp.error_code != "OK":
            click.secho(f"Search failed: {resp.error_code}", fg="red")
            return

        click.echo(f"\nSearch Query: '{query}' (Found {resp.total_hits} results in {resp.execution_time_ms:.2f} ms)\n")
        click.echo("-" * 75)
        click.echo(f"{'Rank':<5} {'Score':<10} {'Shard':<8} {'Ver':<6} {'Doc ID':<20} {'Snippet'}")
        click.echo("-" * 75)
        for idx, item in enumerate(resp.results, start=1):
            snippet = item.snippet.replace("\n", " ")[:35] + "..." if len(item.snippet) > 35 else item.snippet
            click.echo(
                f"{idx:<5} {item.score:<10.4f} {item.shard_id:<8} {item.version:<6} {item.doc_id:<20} {snippet}"
            )
        click.echo("-" * 75)
    except Exception as e:
        click.secho(f"Error communicating with coordinator at {target}: {e}", fg="red")
    finally:
        client.close()


@cli.command()
@click.option("--doc-id", "-i", required=True, help="Unique document identifier")
@click.option("--content", "-c", required=True, help="Text content of the document")
@click.pass_context
def insert(ctx, doc_id: str, content: str) -> None:
    """Insert a new document into the distributed search engine."""
    target = ctx.obj["coordinator"]
    client = SearchEngineClient(target)
    try:
        resp = client.insert(doc_id=doc_id, content=content)
        if resp.success:
            click.secho(
                f"Successfully inserted '{doc_id}' -> Shard {resp.shard_id} (Seq: {resp.sequence_number}, Ver: {resp.version})",
                fg="green",
            )
        else:
            click.secho(f"Insert failed: {resp.error_code} - {resp.error_message}", fg="red")
    except Exception as e:
        click.secho(f"Error: {e}", fg="red")
    finally:
        client.close()


@cli.command()
@click.option("--doc-id", "-i", required=True, help="Unique document identifier")
@click.option("--content", "-c", required=True, help="Updated document text content")
@click.option("--version", "-v", type=int, required=True, help="Expected current document version")
@click.pass_context
def replace(ctx, doc_id: str, content: str, version: int) -> None:
    """Replace an existing document with monotonic version verification."""
    target = ctx.obj["coordinator"]
    client = SearchEngineClient(target)
    try:
        resp = client.replace(doc_id=doc_id, content=content, expected_version=version)
        if resp.success:
            click.secho(
                f"Successfully updated '{doc_id}' -> Shard {resp.shard_id} (Seq: {resp.sequence_number}, New Ver: {resp.version})",
                fg="green",
            )
        else:
            click.secho(f"Replace failed: {resp.error_code} - {resp.error_message}", fg="red")
    except Exception as e:
        click.secho(f"Error: {e}", fg="red")
    finally:
        client.close()


@cli.command()
@click.option("--doc-id", "-i", required=True, help="Unique document identifier")
@click.option("--version", "-v", type=int, required=True, help="Expected current document version")
@click.pass_context
def delete(ctx, doc_id: str, version: int) -> None:
    """Delete a document, creating a versioned tombstone."""
    target = ctx.obj["coordinator"]
    client = SearchEngineClient(target)
    try:
        resp = client.delete(doc_id=doc_id, expected_version=version)
        if resp.success:
            click.secho(
                f"Successfully deleted '{doc_id}' -> Shard {resp.shard_id} (Seq: {resp.sequence_number}, Tombstone Ver: {resp.version})",
                fg="yellow",
            )
        else:
            click.secho(f"Delete failed: {resp.error_code} - {resp.error_message}", fg="red")
    except Exception as e:
        click.secho(f"Error: {e}", fg="red")
    finally:
        client.close()


@cli.command()
@click.pass_context
def status(ctx) -> None:
    """Inspect cluster topology, shard health, monotonic sequences, and replication lag."""
    target = ctx.obj["coordinator"]
    client = SearchEngineClient(target)
    try:
        resp = client.get_cluster_status()
        health_color = "green" if resp.is_healthy else "red"
        click.secho(
            f"\nCluster Status: {'HEALTHY' if resp.is_healthy else 'DEGRADED'} | Total Docs: {resp.total_cluster_documents}\n",
            fg=health_color,
            bold=True,
        )
        click.echo("-" * 90)
        click.echo(
            f"{'Shard':<7} {'Primary Address':<22} {'Seq':<6} {'Docs':<6} {'Replica Address':<22} {'Applied':<8} {'Lag':<6} {'Replica Status'}"
        )
        click.echo("-" * 90)
        for s in resp.shards:
            p_stat = "UP" if s.is_primary_alive else "DOWN"
            p_str = f"{s.primary_address} ({p_stat})"
            r_str = f"{s.replica_address}"
            click.echo(
                f"Shard {s.shard_id:<2} {p_str:<22} {s.latest_sequence:<6} {s.total_documents:<6} {r_str:<22} {s.replica_applied_sequence:<8} {s.replication_lag:<6} {s.replica_status}"
            )
        click.echo("-" * 90 + "\n")
    except Exception as e:
        click.secho(f"Error: {e}", fg="red")
    finally:
        client.close()


@cli.command()
@click.option("--shard-id", "-s", type=int, required=True, help="Shard ID to recover")
@click.pass_context
def recover(ctx, shard_id: int) -> None:
    """Trigger incremental replica recovery for a specified shard."""
    target = ctx.obj["coordinator"]
    client = SearchEngineClient(target)
    try:
        click.echo(f"Triggering incremental recovery for Shard {shard_id}...")
        resp = client.trigger_recovery(shard_id=shard_id)
        if resp.success:
            click.secho(
                f"Recovery SUCCESS: Shard {shard_id} is now {resp.status} (Replayed {resp.replayed_events} events)",
                fg="green",
                bold=True,
            )
        else:
            click.secho(f"Recovery FAILED: {resp.status} - {resp.error_message}", fg="red", bold=True)
    except Exception as e:
        click.secho(f"Error: {e}", fg="red")
    finally:
        client.close()


if __name__ == "__main__":
    cli()
