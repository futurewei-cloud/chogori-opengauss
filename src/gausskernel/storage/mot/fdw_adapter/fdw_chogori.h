#pragma once

void
k2GetForeignRelSize(PlannerInfo *root,
                RelOptInfo *baserel,
		    Oid foreigntableid);
void
k2GetForeignPaths(PlannerInfo *root,
				   RelOptInfo *baserel,
		  Oid foreigntableid);
ForeignScan *
k2GetForeignPlan(PlannerInfo *root,
				  RelOptInfo *baserel,
				  Oid foreigntableid,
				  ForeignPath *best_path,
				  List *tlist,
		 List *scan_clauses);

void k2CreateTable(CreateForeignTableStmt* obj);
