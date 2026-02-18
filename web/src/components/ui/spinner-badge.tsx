import { Badge } from "@/components/ui/badge"
import { Spinner } from "@/components/ui/spinner"

export function SpinnerBadge() {
    return (
        <div className="flex items-center gap-4 [--radius:1.2rem]">
            <Badge className="gap-2">
                <Spinner size={14} className="text-white/70" />
                Syncing
            </Badge>
            <Badge variant="secondary" className="gap-2">
                <Spinner size={14} className="text-slate-400" />
                Updating
            </Badge>
            <Badge variant="outline" className="gap-2">
                <Spinner size={14} className="text-slate-400" />
                Processing
            </Badge>
        </div>
    )
}
