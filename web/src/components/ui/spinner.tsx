// Shad-CN Spinner component: A loading spinner icon,to show statuses like "Syncing.
import * as React from "react"
import { Loader2 } from "lucide-react"
import { cn } from "@/lib/utils"

export interface SpinnerProps extends React.HTMLAttributes<SVGElement> {
    size?: number
}

function Spinner({ className, size = 16, ...props }: SpinnerProps) {
    return (
        <Loader2
            className={cn("animate-spin", className)}
            size={size}
            {...props}
        />
    )
}

export { Spinner }
