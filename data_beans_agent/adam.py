def adam_html_tool() -> dict:
    """Returns a predefined HTML block for the /adam command."""
    html_content = """<style>
          .adam-test { margin: 10px; padding: 10px; border: 1px solid blue; background-color: lightblue;}
          .adam-test h1 { color: navy; }
        </style>
        <div class="adam-test">
          <h1>Hello from Python</h1>
          <p>This is a HTML test for the <code>/adam</code> command, served via Python.</p>
          <ul><li>Item 1 (Python)</li><li>Item 2 (Python)</li></ul>
        </div>"""
    return {"htmlContent": html_content}