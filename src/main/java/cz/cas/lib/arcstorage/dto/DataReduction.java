package cz.cas.lib.arcstorage.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class DataReduction {

    @NotEmpty
    private List<String> regexes = new ArrayList<>();

    @NotNull
    private DataReductionMode mode;
}
