package main

import (
	"testing"
	"time"
)

func TestMakeFileRecord(t *testing.T) {
	fakeFileName := "/media/MediaVolume/fsgaghahd/jnjfgjhfjfg/Multimedia_Antisocial/Silly_Commission/boyd_paul_someproject/media/filename.wav"
	entry := BackupDebugEntry{FilePath: fakeFileName, Notes: "some notes here"}
	rec := MakeFileRecord(&entry)

	if rec == nil {
		t.Error("MakeFileRecord unexpectedly returned nil")
		t.FailNow()
	}

	if rec.Timestamp.After(time.Now()) || rec.Timestamp == time.Unix(0, 0) {
		t.Errorf("got invalid timestamp %s, expected something before %s", rec.Timestamp.Format(time.RFC3339), time.Now().Format(time.RFC3339))
	}

	if rec.StatErr == "" {
		t.Errorf("expected a 'file not found' error, got nothing")
	}

	if rec.Extension != "wav" {
		t.Errorf("expected extension of WAV, got %s", rec.Extension)
	}

	if rec.Filename != "filename" {
		t.Errorf("expected filename of 'filename', got %s", rec.Filename)
	}

	if rec.WorkingGroup != "Multimedia_Antisocial" {
		t.Errorf("expected workinggroup of Multimedia_Antisocial, got %s", rec.WorkingGroup)
	}

	if rec.Commission != "Silly_Commission" {
		t.Errorf("got unexepcted commission %s", rec.Commission)
	}

	if rec.Project != "boyd_paul_someproject" {
		t.Errorf("got unexpected project %s", rec.Project)
	}

	if rec.WholePath != fakeFileName {
		t.Errorf("wholepath did not match incoming name")
	}

	if rec.Notes != "some notes here" {
		t.Errorf("notes did not match incoming notes, got %s", rec.Notes)
	}
}
