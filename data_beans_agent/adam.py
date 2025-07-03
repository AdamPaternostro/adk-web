import datetime
def adam_html_tool() -> dict:
    """Returns a predefined HTML block for the /adam command."""
    html_content = """<style>
          .adam-test { "margin": "10p"x; "padding": 10px; "border": "1px solid blue"; "background-color": "lightblue";}
          .adam-test h1 { "color": "navy"; }
        </style>
        <div class="adam-test">
          <h1>Hello from Python</h1>
          <p>This is a HTML test for the served via Python.</p>
          <ul>
          <li>One</li>
          <li>Two</li>
          <li>Three</li>
          </ul>
        </div>"""
    return {"html": html_content}