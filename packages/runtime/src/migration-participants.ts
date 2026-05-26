import {
  isMigrationParticipant,
  type IGrainMigrationParticipant,
} from "@tsva/core/grain-migration-participant";
import { getPersistentFields } from "@tsva/core/persistent-state-metadata";

/**
 * The migration participants of a grain instance: the grain itself (when it
 * implements the migration hooks) plus each bound persistent-state facet that
 * does. The same set is asked to dehydrate on the source silo and rehydrate on
 * the target, so the activation's in-memory state moves with it.
 */
export function migrationParticipantsOf(instance: object): IGrainMigrationParticipant[] {
  const participants: IGrainMigrationParticipant[] = [];
  if (isMigrationParticipant(instance)) participants.push(instance);
  for (const field of getPersistentFields(instance)) {
    const facet = (instance as Record<string, unknown>)[field.fieldName];
    if (isMigrationParticipant(facet)) participants.push(facet);
  }
  return participants;
}
