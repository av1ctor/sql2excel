// SPDX-License-Identifier: MIT
// sql2excel - Exports partitioned SQL query results to Microsoft Excel using a template
// Copyright 2022 by André Vicentini
package main

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"github.com/xuri/excelize/v2"
	"gopkg.in/yaml.v3"
)

type Partition struct {
	Type  string
	Begin string
	End   string
}

type Variable struct {
	Row   int
	Col   int
	Value string
}

type Totalization struct {
	Col     int
	Formula string
}

type Config struct {
	Input struct {
		Type    string
		Sources []struct {
			Name      string
			Partition Partition
		}
		Query      string
		TimeFormat string `yaml:"time-format"`
	}
	Output struct {
		Name          string
		Variables     []Variable
		Totalizations []Totalization
	}
	Template struct {
		Path  string
		Sheet string
		Row   int `yaml:"start-row"`
		Col   int `yaml:"start-col"`
	}
}

func LoadConfig(
	File string,
) (Config, error) {
	cfg := Config{}

	data, err := os.ReadFile(File)
	if err != nil {
		return cfg, err
	}

	err = yaml.Unmarshal(data, &cfg)
	if err != nil {
		return cfg, err
	}

	return cfg, nil
}

func OpenDb(
	name string,
) (*sqlx.DB, error) {
	db, err := sqlx.Connect("sqlite3", name)
	if err != nil {
		return nil, err
	}

	return db, nil
}

func LoadTemplate(
	path string,
) (*excelize.File, error) {
	tpl, err := excelize.OpenFile(path)
	if err != nil {
		return tpl, err
	}

	defer func() {
		_ = tpl.Close()
	}()

	return tpl, nil
}

func CreatePartitions(
	part Partition,
) ([]time.Time, error) {
	res := []time.Time{}

	begin, err := time.Parse("2006-01-02T15:04:05", part.Begin+"T00:00:00")
	if err != nil {
		return res, err
	}
	end, err := time.Parse("2006-01-02T15:04:05", part.End+"T23:59:59")
	if err != nil {
		return res, err
	}
	var adder func(time.Time) time.Time

	switch part.Type {
	case "day", "daily":
		adder = func(cur time.Time) time.Time { return cur.AddDate(0, 0, 1) }
	case "month", "monthly":
		adder = func(cur time.Time) time.Time { return cur.AddDate(0, 1, 0) }
	case "year", "yearly":
		adder = func(cur time.Time) time.Time { return cur.AddDate(1, 0, 0) }
	default:
		return res, errors.New("unsupported partition type")
	}

	cur := begin
	for ; cur.Before(end); cur = adder(cur) {
		res = append(res, cur)
	}
	res = append(res, cur)

	return res, nil
}

func CloneTemplate(
	cfg Config,
	num int,
	begin string,
	end string,
) (*excelize.File, error) {
	input, err := ioutil.ReadFile(cfg.Template.Path)
	if err != nil {
		return nil, err
	}

	dst := strings.ReplaceAll(
		strings.ReplaceAll(
			strings.ReplaceAll(
				cfg.Output.Name,
				"{num}",
				fmt.Sprint(num),
			),
			"{part.beg}",
			begin),
		"{part.end}",
		end,
	) + ".xlsx"

	err = ioutil.WriteFile(dst, input, 0644)
	if err != nil {
		return nil, err
	}

	return LoadTemplate(dst)
}

func Process(
	cfg Config,
	db *sqlx.DB,
	total int,
	partitions []time.Time,
) error {
	ExcelCols := []string{
		"A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z",
		"AA", "AB", "AC", "AD", "AE", "AF", "AG", "AH", "AI", "AJ", "AK", "AL", "AM", "AN", "AO", "AP", "AQ", "AR", "AS", "AT", "AU", "AV", "AW", "AX", "AY", "AZ",
	}

	for p := 0; p < len(partitions)-1; p++ {
		begin := partitions[p].Format(cfg.Input.TimeFormat)
		end := partitions[p+1].AddDate(0, 0, -1).Format(cfg.Input.TimeFormat)

		tpl, err := CloneTemplate(cfg, total+p, begin, end)
		if err != nil {
			return err
		}

		query := strings.ReplaceAll(strings.ReplaceAll(cfg.Input.Query, "{part.beg}", begin), "{part.end}", end)
		rows, err := db.Queryx(query)
		if err != nil {
			return err
		}

		fmt.Printf("Processing partition: %s to %s\n", begin, end)

		r := int(cfg.Template.Row)
		for rows.Next() {
			cols, err := rows.SliceScan()
			if err != nil {
				return err
			}

			/*err = tpl.DuplicateRowTo(cfg.Template.Sheet, cfg.Template.Row, r)
			if err != nil {
				return err
			}*/

			c := cfg.Template.Col - 1
			axis := ExcelCols[c] + fmt.Sprint(r)
			err = tpl.SetSheetRow(cfg.Template.Sheet, axis, &cols)
			if err != nil {
				return err
			}

			r++
		}

		for _, variable := range cfg.Output.Variables {
			c := variable.Col - 1
			axis := ExcelCols[c] + fmt.Sprint(variable.Row)
			value := strings.ReplaceAll(
				strings.ReplaceAll(
					variable.Value, "{part.beg}", begin,
				),
				"{part.end}",
				end,
			)
			_ = tpl.SetCellStr(cfg.Template.Sheet, axis, value)
		}

		if len(cfg.Output.Totalizations) > 0 {
			err := tpl.InsertRow(cfg.Template.Sheet, r)
			if err != nil {
				return err
			}
		}

		for _, tot := range cfg.Output.Totalizations {
			c := tot.Col - 1
			axis := ExcelCols[c] + fmt.Sprint(r)
			lastRow := fmt.Sprint(r - 1)
			formula := strings.ReplaceAll(
				tot.Formula, "{rows.last}", lastRow,
			)
			style, _ := tpl.GetCellStyle(cfg.Template.Sheet, ExcelCols[c]+lastRow)
			_ = tpl.SetCellFormula(cfg.Template.Sheet, axis, formula)
			_ = tpl.SetCellStyle(cfg.Template.Sheet, axis, axis, style)
		}

		tpl.Save()
		tpl.Close()

		rows.Close()
	}

	return nil
}

func main() {
	fmt.Println("sql2excel - Exports partitioned SQL query results to Microsoft Excel using a template")
	fmt.Println("Copyright 2022 by André Vicentini")

	if len(os.Args) != 2 {
		log.Fatalf("Error: the yaml config file name must be passed as argument")
	}

	cfg, err := LoadConfig(os.Args[1])
	if err != nil {
		log.Fatalf("Error: %v", err)
	}

	total := 1
	for _, source := range cfg.Input.Sources {
		db, err := OpenDb(source.Name)
		if err != nil {
			log.Fatalf("Error: %v", err)
		}

		partitions, err := CreatePartitions(source.Partition)
		if err != nil {
			log.Fatalf("Error: %v", err)
		}

		err = Process(cfg, db, total, partitions)
		if err != nil {
			log.Fatalf("Error: %v", err)
		}

		db.Close()

		total += len(partitions) - 1
	}

}
