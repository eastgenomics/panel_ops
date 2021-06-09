""" Abomination needed for having tests that are not explicit in the test directory

Example:
R27.1 has Relevant panel in panelapp as a panel.
Scientists recommendation was to use the DDG2P as a panel.

Another abomination: R266.1
R266 is deprecated in Panelapp since Dec 2020. No info is accessible from the API
which is where I get all the panel data. Panelapp say on the webpage (https://panelapp.genomicsengland.co.uk/panels/547/)
to use R80, R81, R83
"""

tests = {
    "R27.1": {
        "gemini_name": (
            "R27.1a_Congenital malformation and dysmorphism syndromes"
            " - microarray and sequencing (DDG2P)_P"
        ),
        "panels": ["484"],
        "tests": []
    },
    "R266.1": {
        "tests": ["R80", "R81", "R83"],
        "gemini_name": "R266.1_Neuromuscular_arthrogryposis",
        "panels": []
    }
}
