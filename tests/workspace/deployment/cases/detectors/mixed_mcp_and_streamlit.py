"""Module with both FastMCP and streamlit — MCP should win."""

import streamlit as st
from fastmcp import FastMCP

mcp = FastMCP("mixed-server")
st.title("Also has streamlit")
