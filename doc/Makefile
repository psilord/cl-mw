first: ref.dvi

TEX2DVI=pdflatex -output-format dvi
TEX2PDF=pdflatex -output-format pdf
LATEX2HTML = latex2html -antialias -split 3
HEVEA = hevea

all: ref.dvi ref.pdf ref.html

ref: ref.dvi

ref.html:
	$(LATEX2HTML) ref

ref.pdf:
	$(TEX2PDF) ref.tex
	$(TEX2PDF) ref.tex -interaction=batchmode > /dev/null 2>&1
	$(TEX2PDF) ref.tex -interaction=batchmode > /dev/null 2>&1

ref.dvi: ref.tex
	$(TEX2DVI) ref.tex
	$(TEX2DVI) ref.tex -interaction=batchmode > /dev/null 2>&1
	$(TEX2DVI) ref.tex -interaction=batchmode > /dev/null 2>&1

hevea: ref.tex
	$(HEVEA) ref.tex

clean:
	rm -rf *.out *.aux *.ind *.log *.dvi *.pdf *.ps *.toc ref *.haux ref.html *.htoc
