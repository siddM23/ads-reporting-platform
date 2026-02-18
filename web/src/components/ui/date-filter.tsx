"use client"

import * as React from "react"
import { CalendarIcon } from "lucide-react"
import { format, subDays } from "date-fns"
import { cn } from "@/lib/utils"
import { Button } from "@/components/ui/button"
import { Calendar } from "@/components/ui/calendar"
import {
    Popover,
    PopoverContent,
    PopoverTrigger,
} from "@/components/ui/popover"
import { Card, CardContent, CardFooter } from "@/components/ui/card"

interface DateFilterProps {
    onRangeChange?: (range: string, customStart?: Date, customEnd?: Date) => void;
    className?: string;
}

import { DateRange } from "react-day-picker"

export function DateFilter({ onRangeChange, className }: DateFilterProps) {
    const [date, setDate] = React.useState<DateRange | undefined>({
        from: subDays(new Date(), 7),
        to: new Date(),
    })
    const [currentMonth, setCurrentMonth] = React.useState<Date>(new Date())
    const [selectedLabel, setSelectedLabel] = React.useState<string>("Last 7 days")

    const presets = [
        { label: "All Ranges", value: 0 },
        { label: "Last 7 days", value: -7 },
        { label: "Last 30 days", value: -30 },
        { label: "Last 6 months", value: -180 },
    ]

    const handlePresetClick = (value: number, label: string) => {
        setSelectedLabel(label)
        if (label === "All Ranges") {
            setDate(undefined)
            if (onRangeChange) onRangeChange(label)
            return
        }

        const end = new Date()
        const start = subDays(new Date(), Math.abs(value))

        setDate({ from: start, to: end })
        setCurrentMonth(new Date(start.getFullYear(), start.getMonth(), 1))

        if (onRangeChange) {
            onRangeChange(label, start, end)
        }
    }

    const handleSelect = (range: DateRange | undefined) => {
        setDate(range)
        if (range?.from && range?.to) {
            const startStr = format(range.from, "MMM d, yyyy")
            const endStr = format(range.to, "MMM d, yyyy")
            const label = `${startStr} - ${endStr}`
            setSelectedLabel(label)
            if (onRangeChange) {
                onRangeChange("Custom", range.from, range.to)
            }
        }
    }

    return (
        <div className={cn("grid gap-2", className)}>
            <Popover>
                <PopoverTrigger asChild>
                    <Button
                        id="date"
                        variant={"outline"}
                        className={cn(
                            "w-[240px] justify-start text-left font-normal border-slate-200 rounded-xl h-10 shadow-sm transition-all hover:bg-slate-50",
                            !date && "text-muted-foreground"
                        )}
                    >
                        <CalendarIcon className="mr-2 h-4 w-4 text-slate-500" />
                        <span className="truncate">
                            {selectedLabel}
                        </span>
                    </Button>
                </PopoverTrigger>
                <PopoverContent className="w-auto p-0 rounded-2xl border-slate-200 shadow-xl" align="end">
                    <Card className="border-0 shadow-none">
                        <div className="flex flex-col gap-2 border-b p-3 bg-slate-50/50 rounded-t-2xl">
                            <div className="grid grid-cols-4 gap-2 w-full">
                                {presets.map((preset) => (
                                    <Button
                                        key={preset.label}
                                        variant="outline"
                                        size="sm"
                                        className="justify-center font-medium border-slate-200 bg-white hover:bg-slate-50 transition-colors rounded-lg"
                                        onClick={() => handlePresetClick(preset.value, preset.label)}
                                    >
                                        {preset.label}
                                    </Button>
                                ))}
                            </div>
                        </div>
                        <CardContent className="p-0">
                            <Calendar
                                mode="range"
                                defaultMonth={date?.from}
                                selected={date}
                                onSelect={handleSelect}
                                numberOfMonths={2}
                                month={currentMonth}
                                onMonthChange={setCurrentMonth}
                                className="p-3"
                            />
                        </CardContent>
                    </Card>
                </PopoverContent>
            </Popover>
        </div>
    )
}
