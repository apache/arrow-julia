using Documenter, DocumenterMarkdown
using Arrow

makedocs(
    format = Markdown(),
    modules = [Arrow],
    pages = [
        "Home" => "index.md",
        "User Manual" => "manual.md",
        "API Reference" => "reference.md"
    ]
)

# Documenter can also automatically deploy documentation to gh-pages.
# See "Hosting Documentation" and deploydocs() in the Documenter manual
# for more information.
#=deploydocs(
    repo = "<repository url>"
)=#
