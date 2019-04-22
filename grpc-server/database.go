package heroball

import (
	"database/sql"

	pb "github.com/heroballapp/server/protobuf"
)

type HeroBallDatabase struct {
	connectionString string
	db               *sql.DB
}

func NewHeroBallDatabase(connStr string) (*HeroBallDatabase, error) {
	return &HeroBallDatabase{}, nil
}

func (database *HeroBallDatabase) Connect(connStr string, db *sql.DB) error {
	if db != nil {
		database.db = db
		return nil
	}
	database.connectionString = connStr
	db, err := sql.Open("postgres", database.connectionString)
	if err != nil {
		return err
	}
	database.db = db
	return nil
}

func (db *HeroBallDatabase) GetPlayerInfo(playerId int32) (*pb.PlayerInfo, error) {

	return nil, nil
}
