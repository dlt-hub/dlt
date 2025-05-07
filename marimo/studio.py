import marimo

__generated_with = "0.13.6"
app = marimo.App(width="medium", css_file="style.css")


@app.cell
def _():
    import marimo as mo
    import dlt
    return (mo,dlt)


@app.cell
def _(mo):
    mo.sidebar(
        [
            mo.md("### dltHub Studio"),
            mo.nav_menu(
                {
                    "#/home": f"{mo.icon('lucide:home')} Home",
                    "#/about": f"{mo.icon('lucide:user')} About",
                    "#/docs": f"{mo.icon('lucide:book-open-text')} dlt Docs",
                },
                orientation="vertical",
            ),
        ]
    )

@app.cell
def _(mo):
    mo.vstack(      
        [mo.image(
            "https://dlthub.com/docs/img/dlthub-logo.png",
            width=100
        ),
        mo.md(r"""
              # Welcome to dltHub Studio
               The hackable data platform for `dlt` developers. Learn how to modify this app and create your personal platform [dlthub.com](https://dlthub.com/docs/studio/overview).
               
               """)
        ],
        gap = 1,
    )
    return


if __name__ == "__main__":
    app.run()
