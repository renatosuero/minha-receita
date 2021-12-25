package transform

import (
	"fmt"
	"io"
	"log"
	"path/filepath"
)

func addBaseCPNJ(srcDir, outDir string, l *lookups) error {
	s, err := newSource(baseCNPJ, srcDir)
	if err != nil {
		return fmt.Errorf("error creating source for partners: %w", err)
	}
	defer s.close()
	for _, a := range s.readers {
		for {
			r, err := a.read()
			if err == io.EOF {
				break
			}
			if err != nil {
				break // do not proceed in case of errors.
			}
			b, err := pathForBaseCNPJ(r[0])
			if err != nil {
				return fmt.Errorf("error getting the path for %s: %w", r[0], err)
			}
			ls, err := filepath.Glob(filepath.Join(outDir, b, "*.json"))
			if err != nil {
				return fmt.Errorf("error in the glob pattern: %w", err)
			}
			if len(ls) == 0 {
				log.Output(2, fmt.Sprintf("No JSON file found for CNPJ base %s", r[0]))
				continue
			}
			for _, f := range ls {
				c, err := companyFromJSON(f)
				if err != nil {
					return fmt.Errorf("error reading company from %s: %w", f, err)
				}
				err = c.baseCNPJ(r, l)
				if err != nil {
					return fmt.Errorf("error filling company from %s: %w", f, err)
				}
				f, err = c.toJSON(outDir)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (c *company) baseCNPJ(r []string, l *lookups) error {
	c.RazaoSocial = r[1]
	codigoNaturezaJuridica, err := toInt(r[2])
	if err != nil {
		return fmt.Errorf("error trying to parse CodigoNaturezaJuridica %s: %w", r[2], err)
	}
	c.CodigoNaturezaJuridica = codigoNaturezaJuridica
	qualificacaoDoResponsavel, err := toInt(r[3])
	if err != nil {
		return fmt.Errorf("error trying to parse QualificacaoDoResponsavel %s: %w", r[3], err)
	}
	c.QualificacaoDoResponsavel = qualificacaoDoResponsavel
	capitalSocial, err := toFloat(r[4])
	if err != nil {
		return fmt.Errorf("error trying to parse CapitalSocial %s: %w", r[4], err)
	}
	c.CapitalSocial = capitalSocial
	err = c.porte(r[5])
	if err != nil {
		return fmt.Errorf("error trying to parse Porte %s: %w", r[5], err)
	}
	enteFederativoResponsavel, err := toInt(r[6])
	if err != nil {
		return fmt.Errorf("error trying to parse EnteFederativoResponsavel%s: %w", r[6], err)
	}
	c.EnteFederativoResponsavel = enteFederativoResponsavel
	natures := l.natures[*c.CodigoNaturezaJuridica]
	if natures != "" {
		c.NaturezaJuridica = &natures
	}
	return nil
}

func (c *company) porte(v string) error {
	i, err := toInt(v)
	if err != nil {
		return fmt.Errorf("error trying to parse CodigoPorte %s: %w", v, err)
	}

	var s string
	switch *i {
	case 0:
		s = "NÃO INFORMADO"
	case 1:
		s = "MICRO EMPRESA"
	case 3:
		s = "EMPRESA DE PEQUENO PORTE"
	case 5:
		s = "DEMAIS"
	}

	c.CodigoPorte = i
	if s != "" {
		c.Porte = &s
	}
	return nil
}
