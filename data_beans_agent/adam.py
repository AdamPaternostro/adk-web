def adam_html() -> str:
    return """<style>
          .adam-test { margin: 10px; padding: 10px; border: 1px solid blue; background-color: lightblue;}
          .adam-test h1 { color: navy; }
        </style>
        <div class="adam-test">
          <h1>Hello from Python</h1>
          <p>This is a hardcoded HTML test for the <code>/adam</code> command.</p>
          <ul><li>Item 1</li><li>Item 2</li></ul>
        </div>"""