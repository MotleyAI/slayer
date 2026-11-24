"""Typed access to an MCP tool result's text."""

from mcp.types import CallToolResult, InputRequiredResult, TextContent


def text_blocks(result: CallToolResult | InputRequiredResult) -> list[TextContent]:
    """``result``'s content, asserting it is a finished tool result of text blocks only."""
    assert isinstance(result, CallToolResult), result
    blocks = [block for block in result.content if isinstance(block, TextContent)]
    assert len(blocks) == len(result.content), result.content
    return blocks
