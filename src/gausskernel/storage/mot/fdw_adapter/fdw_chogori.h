#pragma once

void
k2GetForeignRelSize(PlannerInfo *root,
                RelOptInfo *baserel,
		    Oid foreigntableid);
